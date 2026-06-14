package bitstar

import (
	"context"
	"encoding/json"
	"path"
	"sync"
	"sync/atomic"
	"time"

	"github.com/askerdev/bitstar/filtering"
	storagepb "github.com/askerdev/bitstar/proto/infralenta/storage/v1"
	"github.com/google/uuid"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	maxMemTableSize = 16_384
)

var (
	levelLimit = map[int]uint32{
		0: 256_000,
		1: 1_024_000,
		2: 4_096_000,
	}
)

type writeItem struct {
	item *memTableItem
}

type writeRequest struct {
	in  []*writeItem
	rsp chan writeResponse
}

type writeResponse struct {
	err error
}

type Storage struct {
	cur           atomic.Pointer[memTable]
	pen           atomic.Pointer[[]*roaringIndex]
	compactCh     chan struct{}
	compactDoneCh chan *roaringIndex
	levels        atomic.Pointer[[4]*roaringIndex]

	db      *ydb.Driver
	writeCh chan writeRequest
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

func Open(ctx context.Context, db *ydb.Driver) (*Storage, error) {
	ri, err := fromYdb(ctx, db)
	if err != nil {
		return nil, err
	}

	s := &Storage{
		db:            db,
		writeCh:       make(chan writeRequest),
		stopCh:        make(chan struct{}, 2),
		compactCh:     make(chan struct{}),
		compactDoneCh: make(chan *roaringIndex),
	}

	s.cur.Store(newMemTable())
	s.pen.Store(&[]*roaringIndex{})

	s.levels.Store(&[4]*roaringIndex{nil, nil, nil, ri})

	s.wg.Go(func() {
		s.flushWorker(30 * time.Second)
	})
	s.wg.Go(func() {
		s.writeWorker()
	})

	return s, nil
}

func (s *Storage) Close() error {
	close(s.stopCh)
	s.wg.Wait()
	return nil
}

func (s *Storage) writeWorker() {
	for {
		select {
		case ri := <-s.compactDoneCh:
			oldSlice := *s.pen.Load()
			idx := -1
			for i, t := range oldSlice {
				if t == ri {
					idx = i
					break
				}
			}
			if idx != -1 {
				newSlice := make([]*roaringIndex, 0, len(oldSlice)-1)
				newSlice = append(newSlice, oldSlice[:idx]...)
				newSlice = append(newSlice, oldSlice[idx+1:]...)

				s.pen.Store(&newSlice)
			}
		case <-s.stopCh:
			return
		default:
		}

		select {
		case req := <-s.writeCh:
			req.rsp <- writeResponse{err: s.batchCreateEvents(req.in)}
		case ri := <-s.compactDoneCh:
			oldSlice := *s.pen.Load()
			idx := -1
			for i, t := range oldSlice {
				if t == ri {
					idx = i
					break
				}
			}
			if idx != -1 {
				newSlice := make([]*roaringIndex, 0, len(oldSlice)-1)
				newSlice = append(newSlice, oldSlice[:idx]...)
				newSlice = append(newSlice, oldSlice[idx+1:]...)

				s.pen.Store(&newSlice)
			}
		case <-s.stopCh:
			return
		}
	}
}

func (s *Storage) flushWorker(cron time.Duration) {
	c := 0

	for {
		select {
		case <-time.After(cron):
			pen := s.pen.Load()
			for _, table := range *pen {
				s.mergeLevel(table)
			}
		case <-s.compactCh:
			c++
			if c >= 4 {
				c = 0
				pen := s.pen.Load()
				for _, ri := range *pen {
					s.mergeLevel(ri)
				}
			}
		case <-s.stopCh:
			return
		}
	}
}

func (s *Storage) mergeLevel(ri *roaringIndex) error {
	levelsPtr := s.levels.Load()
	levels := [4]*roaringIndex{
		levelsPtr[0],
		levelsPtr[1],
		levelsPtr[2],
		levelsPtr[3],
	}

	if levels[0] == nil {
		levels[0] = ri
	} else {
		levels[0] = mergeTwoIndices(levels[0], ri)
	}

	for i := range 3 {
		if levels[i] == nil {
			continue
		}

		if limit, ok := levelLimit[i]; ok && levels[i].all.GetCardinality() >= uint64(limit) {
			levels[i+1] = mergeTwoIndices(levels[i], levels[i+1])
			levels[i] = nil
		}
	}

	s.levels.Store(&levels)

	s.compactDoneCh <- ri

	return nil
}

func (s *Storage) BatchCreateEvents(ctx context.Context, in *storagepb.BatchCreateEventsRequest) (*storagepb.BatchCreateEventsResponse, error) {
	resCh := make(chan writeResponse, 1)

	events := make([]*storagepb.Event, 0, len(in.GetRequests()))
	batch := make([]*writeItem, 0, len(in.GetRequests()))
	rows := make([]types.Value, 0, len(in.GetRequests()))

	for _, req := range in.GetRequests() {
		id := uuid.Must(uuid.NewV7())
		event := req.GetEvent()
		event.Id = id.String()

		tags, _ := json.Marshal(event.Tags)
		annotations, _ := json.Marshal(event.Annotations)

		rows = append(rows,
			types.StructValue(
				types.StructFieldValue("resource_type_code", types.UTF8Value(event.GetResource().GetTypeCode())),
				types.StructFieldValue("resource_external_id", types.UTF8Value(event.GetResource().GetExternalId())),
				types.StructFieldValue("start_time", types.TimestampValueFromTime(event.GetStartTime().AsTime())),
				types.StructFieldValue("id", types.UuidValue(id)),
				types.StructFieldValue("title", types.UTF8Value(event.GetTitle())),
				types.StructFieldValue("description", types.UTF8Value(event.GetDescription())),
				types.StructFieldValue("end_time", types.TimestampValueFromTime(event.GetEndTime().AsTime())),
				types.StructFieldValue("tags", types.JSONValueFromBytes(tags)),
				types.StructFieldValue("annotations", types.JSONValueFromBytes(annotations)),
				types.StructFieldValue("created_by", types.BytesValue([]byte(event.GetCreatedBy()))),
				types.StructFieldValue("updated_by", types.BytesValue([]byte(event.GetUpdatedBy()))),
				types.StructFieldValue("create_time", types.TimestampValueFromTime(event.GetCreateTime().AsTime())),
				types.StructFieldValue("update_time", types.TimestampValueFromTime(event.GetUpdateTime().AsTime())),
			),
		)

		batch = append(batch, &writeItem{item: newMemTableItem(event)})
	}

	err := s.db.Table().
		BulkUpsert(
			ctx,
			path.Join(s.db.Name(), "events"),
			table.BulkUpsertDataRows(types.ListValue(rows...)),
			table.WithIdempotent(),
		)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "write fail: %v", err)
	}

	select {
	case s.writeCh <- writeRequest{in: batch, rsp: resCh}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	select {
	case res := <-resCh:
		if res.err != nil {
			return nil, status.Errorf(codes.Internal, "write fail: %v", res.err)
		}
		return &storagepb.BatchCreateEventsResponse{Events: events}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *Storage) batchCreateEvents(in []*writeItem) error {
	curTable := s.cur.Load()
	curTable.wg.Add(1)

	if curTable.size() >= maxMemTableSize {
		curTable.wg.Done()

		newTable := newMemTable()
		s.cur.Store(newTable)

		curTable.wg.Wait()
		ri, _ := fromMemTable(curTable)

		oldSlice := *s.pen.Load()
		newSlice := make([]*roaringIndex, len(oldSlice)+1)
		copy(newSlice, oldSlice)
		newSlice[len(oldSlice)] = ri
		s.pen.Store(&newSlice)

		select {
		case s.compactCh <- struct{}{}:
		default:
		}

		curTable = newTable
		curTable.wg.Add(1)
	}

	for _, wi := range in {
		curTable.putItem(wi.item)
	}

	curTable.wg.Done()

	return nil
}

func (s *Storage) ListEvents(ctx context.Context, in *storagepb.ListEventsRequest) (*storagepb.ListEventsResponse, error) {
	filter, err := filtering.ParseFilter(in.GetFilter())
	if err != nil {
		return nil, err
	}

	snapshotCur := s.cur.Load()
	snapshotPen := s.pen.Load()
	snapshotLevels := s.levels.Load()

	stats := &storagepb.Stats{
		CurrentMemTableSize:       int32(snapshotCur.count),
		PendingIndexCount:         int32(len(*snapshotPen)),
		PendingIndexCardinalities: make(map[int32]int32),
		LevelCardinalities:        make(map[int32]int32),
	}

	for i, ri := range *snapshotPen {
		if ri == nil {
			stats.PendingIndexCardinalities[int32(i)] = 0
		} else {
			stats.PendingIndexCardinalities[int32(i)] = int32(ri.all.GetCardinality())
		}
	}

	for i, ri := range *snapshotLevels {
		if ri == nil {
			stats.LevelCardinalities[int32(i)] = 0
		} else {
			stats.LevelCardinalities[int32(i)] = int32(ri.all.GetCardinality())
		}
	}

	eg := &errgroup.Group{}

	var scanKeys []EventKey
	var hasScanNext bool

	eg.Go(func() error {
		sq := MemTableQuery{
			PageToken: in.GetPageToken(),
			PageSize:  int(in.GetPageSize()),
			StartTime: in.GetStartTime().AsTime(),
			EndTime:   in.GetEndTime().AsTime(),
			Filter:    filter,
		}

		keys, hasNext, err := sq.Do([]*memTable{snapshotCur})
		if err != nil {
			return err
		}

		scanKeys = keys
		hasScanNext = hasNext

		return nil
	})

	var indexKeys []EventKey
	var hasIndexNext bool

	eg.Go(func() error {
		iq := IndexQuery{
			PageToken: in.GetPageToken(),
			PageSize:  int(in.GetPageSize()),
			StartTime: in.GetStartTime().AsTime(),
			EndTime:   in.GetEndTime().AsTime(),
			Filter:    filter,
		}

		levels := *snapshotLevels
		ris := make([]*roaringIndex, 0, len(levels)+len(*snapshotPen))
		ris = append(ris, (*snapshotLevels)[:]...)
		ris = append(ris, *snapshotPen...)

		keys, hasNext, err := iq.Do(ris)
		if err != nil {
			return err
		}

		indexKeys = keys
		hasIndexNext = hasNext

		return nil
	})

	if err := eg.Wait(); err != nil {
		return nil, status.Errorf(codes.Internal, "list fail: %v", err)
	}

	keys := mergeKeysLimited(scanKeys, indexKeys, int(in.GetPageSize()))
	if len(keys) == 0 {
		return &storagepb.ListEventsResponse{Stats: stats}, nil
	}

	tableKeys := make([]types.Value, 0, len(keys))
	for _, key := range keys {
		tableKeys = append(tableKeys, types.StructValue(
			types.StructFieldValue("resource_type_code", types.UTF8Value(key.ResourceTypeCode)),
			types.StructFieldValue("resource_external_id", types.UTF8Value(key.ResourceExternalID)),
			types.StructFieldValue("start_time", types.TimestampValueFromTime(key.StartTime)),
			types.StructFieldValue("id", types.UuidValue(uuid.MustParse(key.ID))),
		))
	}

	opts := []options.ReadRowsOption{options.ReadColumns(eventsCols...)}

	result, err := s.db.Table().
		ReadRows(
			ctx,
			path.Join(s.db.Name(), "events"),
			types.ListValue(tableKeys...),
			opts,
			table.WithIdempotent(),
		)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list fail: %v", err)
	}

	eventsMap := make(map[string]*storagepb.Event, len(keys))
	err = forEachEvent(ctx, result, func(event *storagepb.Event) error {
		eventsMap[event.Id] = event
		return nil
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "scan fail: %v", err)
	}

	events := make([]*storagepb.Event, 0, len(keys))
	for _, key := range keys {
		if ev, ok := eventsMap[key.ID]; ok {
			events = append(events, ev)
		}
	}

	var nextPageToken string
	if hasScanNext || hasIndexNext {
		var err error
		nextPageToken, err = EncodePageToken(keys[len(keys)-1])
		if err != nil {
			return nil, status.Errorf(codes.Internal, "list fail: %v", err)
		}
	}

	return &storagepb.ListEventsResponse{
		Events:        events,
		NextPageToken: nextPageToken,
		Stats:         stats,
	}, nil
}
