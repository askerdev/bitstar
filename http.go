package bitstar

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/askerdev/bitstar/bsi"
	"github.com/dgraph-io/badger/v4"
	"go.uber.org/zap"
)

type Handler struct {
	log *zap.SugaredLogger

	mutex *sync.RWMutex

	storage     *badger.DB
	startTime   *bsi.BSI
	endTime     *bsi.BSI
	tags        map[string]*roaring.Bitmap
	annotations map[Pair]*roaring.Bitmap

	mux *http.ServeMux
}

func NewHandler(
	log *zap.SugaredLogger,
) *Handler {
	opts := badger.DefaultOptions("badger").
		WithLogger(&BadgerZapAdapter{Sugar: log})

	db, err := badger.Open(opts)
	if err != nil {
		panic(err)
	}

	h := &Handler{
		log:         log,
		mux:         http.NewServeMux(),
		mutex:       &sync.RWMutex{},
		storage:     db,
		startTime:   bsi.NewDefaultBSI(),
		endTime:     bsi.NewDefaultBSI(),
		tags:        make(map[string]*roaring.Bitmap),
		annotations: make(map[Pair]*roaring.Bitmap),
	}

	if err := h.indexEvents(); err != nil {
		panic(err)
	}

	h.mux.HandleFunc("POST /v1/events:batchCreate", h.batchCreate())
	h.mux.HandleFunc("GET /v1/events", h.list())

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
			return
		}

		h.log.Debug("recv batch create events request", "len", len(batchRequest.Requests))

		events := make([]*Event, 0, len(batchRequest.Requests))

		seq, err := h.storage.GetSequence([]byte("seq_events"), 1000)
		defer seq.Release()

		err = func() error {
			txn := h.storage.NewTransaction(true)
			for _, request := range batchRequest.Requests {
				id, err := seq.Next()
				if err != nil {
					return err
				}
				request.Event.ID = uint32(id)

				key := eventKey(request.Event.ID)
				value, err := json.Marshal(request.Event)
				if err != nil {
					return err
				}

				if err := txn.Set(key, value); errors.Is(err, badger.ErrTxnTooBig) {
					if err := txn.Commit(); err != nil {
						return err
					}

					txn = h.storage.NewTransaction(true)

					if err := txn.Set(key, value); err != nil {
						return err
					}
				} else if err != nil {
					return err
				}

				h.indexEventLock(request.Event)

				events = append(events, request.Event)
			}
			return txn.Commit()
		}()
		if err != nil {
			h.log.Error("storage update fail", "err", err)
			http.Error(w, "storage update fail", http.StatusInternalServerError)
			return
		}

		writeJSON(w, http.StatusCreated, &BatchCreateEventResponse{
			Events: events,
		})
	}
}

func (h *Handler) list() http.HandlerFunc {
	type ListEventsRequest struct {
		StartTime time.Time `json:"start_time"`
		EndTime   time.Time `json:"end_time"`
		Filter    Filter    `json:"filter"`
	}

	type ListEventsResponse struct {
		Events []*Event `json:"events"`
	}

	return func(w http.ResponseWriter, r *http.Request) {
		var request ListEventsRequest
		if err := readJSON(w, r, &request); err != nil {
			return
		}

		h.log.Debug("recv list events request")

		h.mutex.RLock()
		defer h.mutex.RUnlock()

		var res *roaring.Bitmap
		for _, and := range request.Filter.And {
			var mid *roaring.Bitmap
			for _, or := range and.Or {
				if rb, ok := h.tags[or.Tag]; ok {
					if mid == nil {
						mid = rb.Clone()
					} else {
						mid.Or(rb)
					}
				}
				if rb, ok := h.annotations[or.Annotation]; ok {
					if mid == nil {
						mid = rb.Clone()
					} else {
						mid.Or(rb)
					}
				}
			}
			if res == nil {
				res = mid
			} else if mid != nil {
				res.And(mid)
			}
		}

		if !res.IsEmpty() {
			h.log.Debug("equality filtered", "bitmap", res.String())
		}

		// start_time <= :end_time
		startTime := h.startTime.CompareValue(4, bsi.LT, request.EndTime.Unix(), 0, nil)
		startTime.Or(h.startTime.CompareValue(4, bsi.EQ, request.EndTime.Unix(), 0, nil))
		if res == nil {
			res = startTime
		} else {
			res.And(startTime)
		}

		if !res.IsEmpty() {
			h.log.Debug("start_time filtered", "bitmap", res.String())
		}

		// end_time >= :start_time
		endTime := h.endTime.CompareValue(4, bsi.GT, request.StartTime.Unix(), 0, nil)
		endTime.Or(h.endTime.CompareValue(4, bsi.EQ, request.StartTime.Unix(), 0, nil))
		res.And(endTime)

		if !res.IsEmpty() {
			h.log.Debug("end_time filtered", "bitmap", res.String())
		}

		events := []*Event{}

		err := h.storage.View(func(txn *badger.Txn) error {
			opts := badger.DefaultIteratorOptions
			opts.PrefetchSize = 10
			it := txn.NewIterator(opts)
			defer it.Close()

			if !res.IsEmpty() {
				minID := res.Minimum()
				it.Seek(eventKey(minID))
			} else {
				it.Rewind()
			}

			for ; it.ValidForPrefix([]byte("events_")); it.Next() {
				item := it.Item()
				id, err := eventID(item.Key())
				if err != nil {
					return err
				}

				if !res.IsEmpty() {
					if id > res.Maximum() {
						return nil
					} else if !res.Contains(id) {
						continue
					}
				}

				var event *Event
				err = item.Value(func(v []byte) error {
					return json.Unmarshal(v, &event)
				})
				if err != nil {
					return err
				}

				events = append(events, event)
			}

			return nil
		})
		if err != nil {
			h.log.Error("storage view fail", "err", err)
			http.Error(w, "storage view fail", http.StatusInternalServerError)
			return
		}

		slices.SortFunc(events, func(a *Event, b *Event) int {
			if a.StartTime.After(b.StartTime) ||
				a.StartTime.Equal(b.StartTime) && a.ID > b.ID {
				return -1
			}
			return 1
		})

		writeJSON(w, http.StatusOK, &ListEventsResponse{
			Events: events,
		})
	}
}

func (h *Handler) indexEvents() error {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	h.log.Debug("start initial indexing")
	return h.storage.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 512
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.ValidForPrefix([]byte("events_")); it.Next() {
			var event *Event
			err := it.Item().Value(func(v []byte) error {
				return json.Unmarshal(v, &event)
			})
			if err != nil {
				return err
			}
			if event.ID%100 == 0 {
				h.log.Debugf("indexed up to %d", event.ID)
			}
			h.indexEvent(event)
		}
		h.log.Debug("end initial indexing")
		return nil
	})
}

func (h *Handler) indexEventLock(event *Event) {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	h.indexEvent(event)
}

func (h *Handler) indexEvent(event *Event) {
	h.startTime.SetValue(uint64(event.ID), event.StartTime.Unix())
	h.endTime.SetValue(uint64(event.ID), event.EndTime.Unix())

	for _, tag := range event.Tags {
		if _, ok := h.tags[tag]; !ok {
			h.tags[tag] = roaring.New()
		}
		h.tags[tag].Add(event.ID)
	}

	for key, value := range event.Annotations {
		pair := Pair{Key: key, Value: value}
		if _, ok := h.annotations[pair]; !ok {
			h.annotations[pair] = roaring.New()
		}
		h.annotations[pair].Add(event.ID)
	}
}

func eventKey(id uint32) []byte {
	return []byte("events_" + strconv.FormatUint(uint64(id), 10))
}

func eventID(key []byte) (uint32, error) {
	idString := strings.TrimPrefix(string(key), "events_")
	id, err := strconv.ParseUint(idString, 10, 32)
	if err != nil {
		return 0, err
	}
	return uint32(id), nil
}

func readJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	if err := json.NewDecoder(r.Body).Decode(&dst); err != nil {
		http.Error(w, "failed to decode request", http.StatusBadRequest)
		return fmt.Errorf("failed to decode request")
	}
	return nil
}

func writeJSON(w http.ResponseWriter, statusCode int, data any) {
	w.WriteHeader(statusCode)
	if data == nil {
		return
	}
	bytes, err := json.Marshal(data)
	if err != nil {
		http.Error(w, "failed to encode response", http.StatusInternalServerError)
		return
	}
	w.Write(bytes)
}
