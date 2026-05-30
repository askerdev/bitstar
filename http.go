package bitstar

import (
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/askerdev/bitstar/bsi"
)

type Handler struct {
	mutex *sync.RWMutex

	sequence    uint32
	storage     map[uint32]*Event
	startTime   *bsi.BSI
	endTime     *bsi.BSI
	tags        map[string]*roaring.Bitmap
	annotations map[Pair]*roaring.Bitmap

	mux *http.ServeMux
}

func NewHandler() *Handler {
	h := &Handler{
		mux:         http.NewServeMux(),
		mutex:       &sync.RWMutex{},
		storage:     make(map[uint32]*Event),
		startTime:   bsi.NewDefaultBSI(),
		endTime:     bsi.NewDefaultBSI(),
		tags:        make(map[string]*roaring.Bitmap),
		annotations: make(map[Pair]*roaring.Bitmap),
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

		h.mutex.Lock()
		defer h.mutex.Unlock()

		events := make([]*Event, 0, len(batchRequest.Requests))

		for _, request := range batchRequest.Requests {
			request.Event.ID = h.sequence
			h.sequence++

			h.storage[request.Event.ID] = request.Event

			h.startTime.SetValue(uint64(request.Event.ID), request.Event.StartTime.Unix())
			h.endTime.SetValue(uint64(request.Event.ID), request.Event.EndTime.Unix())

			for _, tag := range request.Event.Tags {
				if _, ok := h.tags[tag]; !ok {
					h.tags[tag] = roaring.New()
				}
				h.tags[tag].Add(request.Event.ID)
			}

			for key, value := range request.Event.Annotations {
				pair := Pair{Key: key, Value: value}
				if _, ok := h.annotations[pair]; !ok {
					h.annotations[pair] = roaring.New()
				}
				h.annotations[pair].Add(request.Event.ID)
			}

			events = append(events, request.Event)
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

		// start_time <= :end_time
		startTime := h.startTime.CompareValue(4, bsi.LT, request.EndTime.Unix(), 0, nil)
		startTime.Or(h.startTime.CompareValue(4, bsi.EQ, request.EndTime.Unix(), 0, nil))
		if res == nil {
			res = startTime
		} else {
			res.And(startTime)
		}

		// end_time >= :start_time
		endTime := h.endTime.CompareValue(4, bsi.GT, request.StartTime.Unix(), 0, nil)
		endTime.Or(h.endTime.CompareValue(4, bsi.EQ, request.StartTime.Unix(), 0, nil))
		res.And(endTime)

		events := []*Event{}

		for _, event := range h.storage {
			if res == nil || res.Contains(event.ID) {
				events = append(events, event)
			}
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
