package bitstar

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"net/http"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/askerdev/bitstar/bsi"
	"github.com/askerdev/bitstar/filtering"
	storagepb "github.com/askerdev/bitstar/proto/infralenta/storage/v1"
	"github.com/askerdev/bitstar/wal"
	"github.com/dgraph-io/badger/v4/skl"
	"github.com/dgraph-io/badger/v4/y"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type EventService struct {
	log *zap.SugaredLogger
	mux *http.ServeMux
	skl *skl.Skiplist

	all         *roaring.Bitmap
	startTime   *bsi.BSI
	endTime     *bsi.BSI
	tags        map[string]*roaring.Bitmap
	annotations map[Pair]*roaring.Bitmap

	keys    map[uint32][]byte
	indexes map[string]uint32

	wal *wal.WAL

	indexOnce *sync.Once
}

func NewEventService(log *zap.SugaredLogger, walDir string) *EventService {
	es := &EventService{
		log:         log,
		mux:         http.NewServeMux(),
		skl:         skl.NewSkiplist(8192 << 20),
		startTime:   bsi.NewDefaultBSI(),
		endTime:     bsi.NewDefaultBSI(),
		tags:        make(map[string]*roaring.Bitmap),
		annotations: make(map[Pair]*roaring.Bitmap),
		all:         roaring.New(),
		keys:        make(map[uint32][]byte),
		indexes:     make(map[string]uint32),
		wal:         wal.Must(wal.New(filepath.Join(walDir, "wal"))),
		indexOnce:   &sync.Once{},
	}

	if err := es.recover(); err != nil {
		panic(err)
	}

	return es
}

func (h *EventService) BatchCreateEvents(ctx context.Context, in *storagepb.BatchCreateEventsRequest) (*storagepb.BatchCreateEventsResponse, error) {
	events := []*storagepb.Event{}

	err := traceErr(h.log, "batchCreate", func() error {
		for _, request := range in.GetRequests() {
			name := uuid.Must(uuid.NewV7())

			request.Event.Id = name.String()

			key := encodeKey(request.Event.StartTime.AsTime(), name)
			value, err := proto.Marshal(request.Event)
			if err != nil {
				return err
			}

			entry := make([]byte, len(key)+4+len(value))
			copy(entry[0:24], key)
			binary.BigEndian.PutUint32(entry[24:24+4], uint32(len(value)))
			copy(entry[24+4:], value)

			if err := h.wal.Append(entry); err != nil {
				return err
			}

			// TODO: first write all entries to WAL, only then
			// write to memory. Handle transactionally and truncate
			// batch in WAL if error occured while writing to WAL.
			// Important for idempotency guarantees
			h.skl.Put(key, y.ValueStruct{
				Value:    value,
				Meta:     0,
				UserMeta: 0,
			})

			events = append(events, request.Event)
		}

		return h.wal.Sync()
	})
	if err != nil {
		h.log.Errorw("batch create fail", "err", err)
		return nil, status.Error(codes.Internal, "batch create fail")
	}

	runtime.GC()

	return &storagepb.BatchCreateEventsResponse{Events: events}, nil
}

func (h *EventService) ListEvents(ctx context.Context, in *storagepb.ListEventsRequest) (*storagepb.ListEventsResponse, error) {
	h.indexOnce.Do(func() {
		h.index()
	})

	if in.PageSize <= 0 || in.PageSize > 512 {
		in.PageSize = 512
	}

	if in.EndTime.AsTime().IsZero() {
		in.EndTime = timestamppb.New(time.Unix(math.MaxUint32-1, 0))
	}

	events, nextPageToken, err := h.bitmapFilter(in)
	if err != nil {
		h.log.Errorw("filter fail", "err", err)
		return nil, status.Error(codes.Internal, "filter fail")
	}

	return &storagepb.ListEventsResponse{
		Events:        events,
		NextPageToken: nextPageToken,
	}, nil
}

func (h *EventService) bitmapFilter(request *storagepb.ListEventsRequest) ([]*storagepb.Event, string, error) {
	var res *roaring.Bitmap

	err := traceErr(h.log, "filter mark", func() error {
		filter, err := filtering.ParseFilter(request.GetFilter())
		if err != nil {
			return err
		}
		res, err = h.evalFilter(filter)
		return err
	})
	if err != nil {
		return nil, "", err
	}

	trace(h.log, "filter range start_time", func() {
		res = h.startTime.CompareValue(4, bsi.LE, request.EndTime.AsTime().Unix(), 0, res)
	})

	trace(h.log, "filter range end_time", func() {
		res = h.endTime.CompareValue(4, bsi.GE, request.StartTime.AsTime().Unix(), 0, res)
	})

	iter := res.Iterator()
	if len(request.PageToken) > 0 {
		maxLen := base64.StdEncoding.DecodedLen(len(request.PageToken))
		buf := make([]byte, maxLen)
		if _, err := base64.StdEncoding.Decode(buf, []byte(request.PageToken)); err != nil {
			panic(err)
		}
		iter.AdvanceIfNeeded(h.indexes[string(buf)])
	}

	events := make([]*storagepb.Event, 0, request.PageSize)

	err = traceErr(h.log, "collect result", func() error {
		for iter.HasNext() && len(events) < int(request.PageSize) {
			index := iter.Next()
			event := &storagepb.Event{}
			if err := proto.Unmarshal(h.skl.Get(h.keys[index]).Value, event); err != nil {
				return err
			}
			events = append(events, event)
		}
		return nil
	})
	if err != nil {
		return nil, "", err
	}

	var nextPageToken string
	if iter.HasNext() {
		last := events[len(events)-1]
		nextPageToken = base64.StdEncoding.EncodeToString(encodeKey(last.StartTime.AsTime(), uuid.MustParse(last.GetId())))
	}

	return events, nextPageToken, nil
}

func (h *EventService) evalFilter(filter *filtering.Filter) (*roaring.Bitmap, error) {
	if filter == nil || filter.Expression == nil {
		return nil, nil
	}
	var res *roaring.Bitmap
	for _, seq := range filter.Expression.Sequences {
		bitmap, err := h.evalSequence(seq)
		if err != nil {
			return nil, err
		}
		if res == nil {
			res = bitmap
		} else if bitmap != nil {
			res.And(bitmap)
		}
	}
	return res, nil
}

func (h *EventService) evalSequence(sequence *filtering.Sequence) (*roaring.Bitmap, error) {
	if sequence == nil {
		return nil, nil
	}
	var res *roaring.Bitmap
	for _, factor := range sequence.Factors {
		bitmap, err := h.evalFactor(factor)
		if err != nil {
			return nil, err
		}
		if res == nil {
			res = bitmap
		} else if bitmap != nil {
			res.And(bitmap)
		}
	}
	return res, nil
}

func (h *EventService) evalFactor(factor *filtering.Factor) (*roaring.Bitmap, error) {
	if factor == nil {
		return nil, nil
	}
	var res *roaring.Bitmap
	for _, term := range factor.Terms {
		bitmap, err := h.evalTerm(term)
		if err != nil {
			return nil, err
		}
		if res == nil {
			res = bitmap
		} else if bitmap != nil {
			res.Or(bitmap)
		}
	}
	return res, nil
}

func (h *EventService) evalTerm(term *filtering.Term) (*roaring.Bitmap, error) {
	if term == nil {
		return nil, nil
	}
	bitmap, err := h.evalSimple(term.Simple)
	if err != nil {
		return nil, err
	}
	if bitmap == nil {
		return nil, nil
	}
	if term.Negated {
		allNot := h.all.Clone()
		allNot.AndNot(bitmap)
		bitmap = allNot
	}
	return bitmap, nil
}

func (h *EventService) evalSimple(simple *filtering.Simple) (*roaring.Bitmap, error) {
	if simple == nil {
		return nil, nil
	}
	switch {
	case simple.Composite != nil:
		return h.evalFilter(&filtering.Filter{Expression: simple.Composite})
	case simple.Restriction != nil:
		return h.evalRestriction(simple.Restriction)
	}
	return nil, nil
}

func (h *EventService) evalRestriction(r *filtering.Restriction) (*roaring.Bitmap, error) {
	if r.Comparable == nil || r.Comparable.Member == nil || r.Comparable.Member.Value == nil {
		return nil, errors.New("invalid restriction")
	}

	fieldName := r.Comparable.Member.Value.Value
	switch fieldName {
	case "tags":
		if r.Arg == nil || r.Arg.Comparable == nil || r.Arg.Comparable.Member == nil || r.Arg.Comparable.Member.Value == nil {
			return nil, errors.New("`tags` requires argument")
		}
		if !r.Arg.Comparable.Member.Value.Quoted {
			return nil, errors.New("`tags` value type mismatch, expected string, got not quoted value")
		}
		tagName := r.Arg.Comparable.Member.Value.Value
		if rb, ok := h.tags[tagName]; ok {
			return rb.Clone(), nil
		}
		return roaring.New(), nil
	case "annotations":
		if len(r.Comparable.Member.Fields) == 0 || r.Arg == nil || r.Arg.Comparable == nil || r.Arg.Comparable.Member == nil || r.Arg.Comparable.Member.Value == nil {
			return nil, errors.New("`annotations` requires key and value")
		}
		if !r.Arg.Comparable.Member.Value.Quoted {
			return nil, errors.New("`annotations` value type mismatch, expected string, got not quoted value")
		}
		key := r.Comparable.Member.Fields[0].Value
		value := r.Arg.Comparable.Member.Value.Value
		pair := Pair{Key: key, Value: value}
		if rb, ok := h.annotations[pair]; ok {
			return rb.Clone(), nil
		}
		return roaring.New(), nil
	default:
		return nil, fmt.Errorf("unknown field %q", fieldName)
	}
}

func (h *EventService) recover() error {
	for i := uint64(1); i <= h.wal.LastIndex(); i++ {
		entry, err := h.wal.GetEntry(i)
		if err != nil {
			return err
		}

		key := make([]byte, 24)
		copy(key, entry[0:24])

		size := binary.BigEndian.Uint32(entry[24 : 24+4])
		value := make([]byte, size)
		copy(value, entry[24+4:])

		h.skl.Put(key, y.ValueStruct{
			Value:    value,
			Meta:     0,
			UserMeta: 0,
		})
	}

	return nil
}

func (h *EventService) index() {
	it := h.skl.NewIterator()
	defer it.Close()

	index := uint32(0)
	for it.SeekToLast(); it.Valid(); it.Prev() {
		event := &storagepb.Event{}
		if err := proto.Unmarshal(it.Value().Value, event); err != nil {
			panic(err)
		}

		key := it.Key()
		h.keys[index] = key
		h.indexes[string(key)] = index

		h.all.Add(index)

		h.startTime.SetValue(uint64(index), event.StartTime.AsTime().Unix())
		h.endTime.SetValue(uint64(index), event.EndTime.AsTime().Unix())

		for _, tag := range event.Tags {
			if _, ok := h.tags[tag]; !ok {
				h.tags[tag] = roaring.New()
			}
			h.tags[tag].Add(index)
		}

		for key, value := range event.Annotations {
			pair := Pair{Key: key, Value: value}
			if _, ok := h.annotations[pair]; !ok {
				h.annotations[pair] = roaring.New()
			}
			h.annotations[pair].Add(index)
		}

		index++
	}

	runtime.GC()
}

func trace(log *zap.SugaredLogger, name string, f func()) {
	start := time.Now()

	f()

	log.Debugw("trace", "name", name, "elapsed", time.Since(start).String())
}

func traceErr(log *zap.SugaredLogger, name string, f func() error) error {
	start := time.Now()

	err := f()

	log.Debugw("trace", "name", name, "elapsed", time.Since(start).String())

	return err
}
