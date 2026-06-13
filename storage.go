package bitstar

import (
	"context"
	"encoding/base64"
	"sync"
	"sync/atomic"
	"time"

	"github.com/askerdev/bitstar/filtering"
	storagepb "github.com/askerdev/bitstar/proto/infralenta/storage/v1"
	"github.com/dgraph-io/badger/v4/skl"
	"github.com/dgraph-io/badger/v4/y"
	"github.com/google/uuid"
	"go.etcd.io/bbolt"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	arenaSize = int64(256 << 20)
)

var (
	eventsBucket = []byte("events")
	threshold    = int64(arenaSize * 90 / 100)

	levelLimit = map[int]uint32{
		0: 256_000,
		1: 1_024_000,
		2: 4_096_000,
	}
)

type Storage struct {
	cur       atomic.Pointer[MemTable]
	pen       atomic.Pointer[[]*MemTable]
	compactCh chan *MemTable
	stopCh    chan struct{}
	doneCh    chan struct{}
	mu        sync.Mutex
	db        *bbolt.DB
	levels    atomic.Pointer[[4]*roaringIndex]
}

func Open(path string) (*Storage, error) {
	db, err := bbolt.Open(path, 0600, nil)
	if err != nil {
		return nil, err
	}

	err = db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(eventsBucket)
		return err
	})
	if err != nil {
		return nil, err
	}

	ri, err := fromBbolt(db, eventsBucket)
	if err != nil {
		return nil, err
	}

	s := &Storage{
		db:        db,
		stopCh:    make(chan struct{}, 1),
		doneCh:    make(chan struct{}, 1),
		compactCh: make(chan *MemTable, 8),
	}

	s.cur.Store(&MemTable{
		skl: skl.NewSkiplist(arenaSize),
	})

	s.pen.Store(&[]*MemTable{})

	s.levels.Store(&[4]*roaringIndex{nil, nil, nil, ri})

	go s.flushWorker(30 * time.Second)

	return s, nil
}

func (s *Storage) Close() error {
	s.stopCh <- struct{}{}
	<-s.doneCh
	return s.db.Close()
}

func (s *Storage) flushWorker(cron time.Duration) {
	ticker := time.NewTicker(cron)
	defer ticker.Stop()

LOOP:
	for {
		select {
		case <-ticker.C:
			tables := s.pen.Load()
			for _, table := range *tables {
				s.compact(table)
			}
		case table := <-s.compactCh:
			s.compact(table)
		case <-s.stopCh:
			break LOOP
		}
	}

	s.doneCh <- struct{}{}
}

func (s *Storage) compact(table *MemTable) error {
	table.wg.Wait()

	levelsPtr := s.levels.Load()
	levels := [4]*roaringIndex{
		levelsPtr[0],
		levelsPtr[1],
		levelsPtr[2],
		levelsPtr[3],
	}

	ri, err := fromSkipList(table.skl)
	if err != nil {
		return err
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
	s.removeFromPending(table)

	return nil
}

func (s *Storage) removeFromPending(table *MemTable) {
	for {
		oldSlicePtr := s.pen.Load()
		if oldSlicePtr == nil {
			return
		}
		oldSlice := *oldSlicePtr

		idx := -1
		for i, t := range oldSlice {
			if t == table {
				idx = i
				break
			}
		}

		if idx == -1 {
			break
		}

		newSlice := make([]*MemTable, 0, len(oldSlice)-1)
		newSlice = append(newSlice, oldSlice[:idx]...)
		newSlice = append(newSlice, oldSlice[idx+1:]...)

		if s.pen.CompareAndSwap(oldSlicePtr, &newSlice) {
			break
		}
	}
}

func (s *Storage) BatchCreateEvents(ctx context.Context, in *storagepb.BatchCreateEventsRequest) (*storagepb.BatchCreateEventsResponse, error) {
	events := []*storagepb.Event{}

	pairs := make([]BytePair, 0, len(in.GetRequests()))

	err := s.db.Batch(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(eventsBucket)

		for _, request := range in.GetRequests() {
			id := uuid.Must(uuid.NewV7())
			request.Event.Id = id.String()

			key := encodeKey(request.Event.StartTime.AsTime(), id)

			val, err := proto.Marshal(request.Event)
			if err != nil {
				return err
			}

			if err := bucket.Put(key, val); err != nil {
				return err
			}

			events = append(events, request.Event)
			pairs = append(pairs, BytePair{
				Key: key,
				Val: val,
			})
		}

		return nil
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "batch create fail: %v", err)
	}

	var batchAllocSize int64
	for _, pair := range pairs {
		// 256 is just a magic number to be sure
		batchAllocSize += int64(len(pair.Key) + len(pair.Val) + 256)
	}

	curTable := s.cur.Load()
	curTable.wg.Add(1)

	if curTable.skl.MemSize()+batchAllocSize >= threshold {
		curTable.wg.Done()
		newTable := &MemTable{skl: skl.NewSkiplist(arenaSize)}
		if s.cur.CompareAndSwap(curTable, newTable) {
			select {
			case s.compactCh <- curTable:
			default:
			}

			for {
				oldSlicePtr := s.pen.Load()
				oldSlice := *oldSlicePtr

				newSlice := make([]*MemTable, len(oldSlice)+1)
				copy(newSlice, oldSlice)
				newSlice[len(oldSlice)] = curTable

				if s.pen.CompareAndSwap(oldSlicePtr, &newSlice) {
					break
				}
			}
			curTable = newTable
		} else {
			curTable = s.cur.Load()
		}
		curTable.wg.Add(1)
	}

	for _, pair := range pairs {
		curTable.skl.Put(pair.Key, y.ValueStruct{
			Value:    pair.Val,
			Meta:     0,
			UserMeta: 0,
		})
	}

	curTable.wg.Done()

	return &storagepb.BatchCreateEventsResponse{
		Events: events,
	}, nil
}

func (s *Storage) ListEvents(ctx context.Context, in *storagepb.ListEventsRequest) (*storagepb.ListEventsResponse, error) {
	filter, err := filtering.ParseFilter(in.GetFilter())
	if err != nil {
		return nil, err
	}

	snapshotCur := s.cur.Load()
	snapshotPen := s.pen.Load()
	snapshotLevels := s.levels.Load()

	eg := &errgroup.Group{}

	var scanKeys [][]byte
	var hasScanNext bool

	eg.Go(func() error {
		sq := ScanQuery{
			PageToken: in.GetPageToken(),
			PageSize:  int(in.GetPageSize()),
			StartTime: in.GetStartTime().AsTime(),
			EndTime:   in.GetEndTime().AsTime(),
			Filter:    filter,
		}

		skls := make([]*skl.Skiplist, 0, 1+len(*snapshotPen))
		skls = append(skls, snapshotCur.skl)
		for _, mt := range *snapshotPen {
			skls = append(skls, mt.skl)
		}

		keys, hasNext, err := sq.Do(skls)
		if err != nil {
			return err
		}

		scanKeys = keys
		hasScanNext = hasNext

		return nil
	})

	var indexKeys [][]byte
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

		keys, hasNext, err := iq.Do(levels[:])
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

	events := make([]*storagepb.Event, 0, len(keys))

	err = s.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(eventsBucket)
		for _, key := range keys {
			event := &storagepb.Event{}
			if err := proto.Unmarshal(bucket.Get(key), event); err != nil {
				return err
			}
			events = append(events, event)
		}
		return nil
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list fail: %v", err)
	}

	var nextPageToken string
	if hasScanNext || hasIndexNext {
		nextPageToken = base64.StdEncoding.EncodeToString(keys[len(keys)-1])
	}

	return &storagepb.ListEventsResponse{
		Events:        events,
		NextPageToken: nextPageToken,
	}, nil
}
