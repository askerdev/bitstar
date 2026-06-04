package bitstar

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"path/filepath"
	"runtime"
	"slices"
	"time"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/askerdev/bitstar/bsi"
	"github.com/askerdev/bitstar/filtering"
	"github.com/askerdev/bitstar/wal"
	"github.com/dgraph-io/badger/v4/skl"
	"github.com/dgraph-io/badger/v4/y"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

type Handler struct {
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
}

func NewHandler(log *zap.SugaredLogger, walDir string) *Handler {
	h := &Handler{
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
	}

	if err := h.recover(); err != nil {
		panic(err)
	}

	h.mux.HandleFunc("POST /v1/events:batchCreate", h.batchCreate())
	h.mux.HandleFunc("GET /v1/events", h.list())
	h.mux.HandleFunc("GET /v1/index", h.index())

	return h
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mux.ServeHTTP(w, r)
}

func (h *Handler) batchCreate() http.HandlerFunc {
	type CreateEventRequest struct {
		Event *Event `json:"event"`
	}

	type BatchCreateEventRequest struct {
		Requests []CreateEventRequest `json:"requests"`
	}

	type BatchCreateEventResponse struct {
		Events []*Event `json:"events"`
	}

	return func(w http.ResponseWriter, r *http.Request) {
		var batchRequest BatchCreateEventRequest
		if err := readJSON(w, r, &batchRequest); err != nil {
			h.log.Error(err)
			return
		}

		events := make([]*Event, 0, len(batchRequest.Requests))
		err := traceErr(h.log, "batchCreate", func() error {
			for _, request := range batchRequest.Requests {
				request.Event.ID = uuid.Must(uuid.NewV7())

				key := encodeKey(request.Event.StartTime, request.Event.ID)
				value := encodeEvent(request.Event)

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
			http.Error(w, "batch create fail", http.StatusInternalServerError)
			return
		}

		h.writeJSON(w, http.StatusCreated, &BatchCreateEventResponse{
			Events: events,
		})

		runtime.GC()
	}
}

type ListEventsRequest struct {
	StartTime        time.Time `json:"start_time"`
	EndTime          time.Time `json:"end_time"`
	PageToken        string    `json:"page_token"`
	PageSize         int       `json:"page_size"`
	Filter           string    `json:"filter"`
	StructuredFilter Filter    `json:"structured_filter"`
}

func (h *Handler) list() http.HandlerFunc {
	type ListEventsResponse struct {
		Events        []*Event `json:"events"`
		NextPageToken string   `json:"next_page_token,omitempty"`
	}

	return func(w http.ResponseWriter, r *http.Request) {
		var request ListEventsRequest
		if err := readJSON(w, r, &request); err != nil {
			h.log.Error(err)
			return
		}

		if request.PageSize <= 0 || request.PageSize > 512 {
			request.PageSize = 512
		}

		if request.EndTime.IsZero() {
			request.EndTime = time.Unix(math.MaxUint32-1, 0)
		}

		events, nextPageToken, err := h.bitmapFilter(&request)
		if err != nil {
			h.log.Errorw("filter fail", "err", err)
			http.Error(w, "filter fail", http.StatusInternalServerError)
			return
		}

		h.writeJSON(w, http.StatusOK, &ListEventsResponse{
			Events:        events,
			NextPageToken: nextPageToken,
		})
	}
}

func (h *Handler) bitmapFilter(request *ListEventsRequest) ([]*Event, string, error) {
	var res *roaring.Bitmap

	err := traceErr(h.log, "filter mark", func() error {
		filter, err := filtering.ParseFilter(request.Filter)
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
		res = h.startTime.CompareValue(4, bsi.LE, request.EndTime.Unix(), 0, res)
	})

	trace(h.log, "filter range end_time", func() {
		res = h.endTime.CompareValue(4, bsi.GE, request.StartTime.Unix(), 0, res)
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

	events := make([]*Event, 0, request.PageSize)

	trace(h.log, "collect result", func() {
		for iter.HasNext() && len(events) < request.PageSize {
			index := iter.Next()

			var event Event
			decodeEvent(h.skl.Get(h.keys[index]).Value, &event)

			events = append(events, &event)
		}
	})

	var nextPageToken string
	if iter.HasNext() {
		last := events[len(events)-1]
		nextPageToken = base64.StdEncoding.EncodeToString(encodeKey(last.StartTime, last.ID))
	}

	return events, nextPageToken, nil
}

func (h *Handler) evalFilter(filter *filtering.Filter) (*roaring.Bitmap, error) {
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
		} else {
			res.And(bitmap)
		}
	}
	return res, nil
}

func (h *Handler) evalSequence(sequence *filtering.Sequence) (*roaring.Bitmap, error) {
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
		} else {
			res.And(bitmap)
		}
	}
	return res, nil
}

func (h *Handler) evalFactor(factor *filtering.Factor) (*roaring.Bitmap, error) {
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
		} else {
			res.Or(bitmap)
		}
	}
	return res, nil
}

func (h *Handler) evalTerm(term *filtering.Term) (*roaring.Bitmap, error) {
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

func (h *Handler) evalSimple(simple *filtering.Simple) (*roaring.Bitmap, error) {
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

func (h *Handler) evalRestriction(r *filtering.Restriction) (*roaring.Bitmap, error) {
	if r.Comparable == nil || r.Comparable.Member == nil || r.Comparable.Member.Value == nil {
		return nil, errors.New("invalid restriction")
	}

	var bitmap *roaring.Bitmap
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
			bitmap = rb.Clone()
		}
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
			bitmap = rb.Clone()
		}
	default:
		return nil, fmt.Errorf("unknown field %q", fieldName)
	}

	return bitmap, nil
}

func (h *Handler) recover() error {
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

func (h *Handler) scanFilter(request *ListEventsRequest) ([]*Event, string) {
	events := make([]*Event, 0, request.PageSize)

	it := h.skl.NewIterator()
	defer it.Close()

	prefix := make([]byte, 24)
	if len(request.PageToken) > 0 {
		maxLen := base64.StdEncoding.DecodedLen(len(request.PageToken))
		buf := make([]byte, maxLen)
		if _, err := base64.StdEncoding.Decode(buf, []byte(request.PageToken)); err != nil {
			panic(err)
		}
		prefix = buf
	} else {
		encodeTime(prefix[0:8], request.EndTime)
		for i := 8; i < 24; i++ {
			prefix[i] = 0xff
		}
	}

	it.Seek(prefix)
	if !it.Valid() {
		it.SeekToLast()
	}

	trace(h.log, "filter", func() {
	LOOP:
		for ; it.Valid() && len(events) <= request.PageSize; it.Prev() {
			var event Event
			decodeEvent(it.Value().Value, &event)

			if event.EndTime.Before(request.StartTime) {
				continue
			}

			for _, and := range request.StructuredFilter.And {
				oneof := false
				for _, or := range and.Or {
					if len(or.Tag) > 0 && slices.Contains(event.Tags, or.Tag) {
						oneof = true
						break
					} else if len(or.Annotation.Key) > 0 {
						v, ok := event.Annotations[or.Annotation.Key]
						if ok && v == or.Annotation.Value {
							oneof = true
							break
						}
					}
				}
				if !oneof {
					continue LOOP
				}
			}

			events = append(events, &event)
		}
	})

	var nextPageToken string
	if it.Valid() {
		last := events[len(events)-1]
		nextPageToken = base64.StdEncoding.EncodeToString(encodeKey(last.StartTime, last.ID))
	}

	return events, nextPageToken
}

func (h *Handler) index() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		it := h.skl.NewIterator()
		defer it.Close()

		index := uint32(0)
		for it.SeekToLast(); it.Valid(); it.Prev() {
			var event Event
			decodeEvent(it.Value().Value, &event)

			key := it.Key()
			h.keys[index] = key
			h.indexes[string(key)] = index

			h.all.Add(index)

			h.startTime.SetValue(uint64(index), event.StartTime.Unix())
			h.endTime.SetValue(uint64(index), event.EndTime.Unix())

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
}

func (h *Handler) writeJSON(w http.ResponseWriter, statusCode int, data any) {
	w.WriteHeader(statusCode)
	if data == nil {
		return
	}
	trace(h.log, "marshal", func() {
		bytes, err := json.Marshal(data)
		if err != nil {
			http.Error(w, "failed to encode response", http.StatusInternalServerError)
			return
		}
		w.Write(bytes)
	})
}

func readJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	if err := json.NewDecoder(r.Body).Decode(&dst); err != nil {
		http.Error(w, "failed to decode request", http.StatusBadRequest)
		return fmt.Errorf("failed to decode request: %w", err)
	}
	return nil
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
