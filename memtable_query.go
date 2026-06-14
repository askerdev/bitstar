package bitstar

import (
	"container/heap"
	"time"

	"github.com/askerdev/bitstar/filtering"
)

type MemTableQuery struct {
	PageSize  int
	PageToken string
	StartTime time.Time
	EndTime   time.Time
	Filter    *filtering.Filter
}

func (mq *MemTableQuery) Do(mts []*memTable) ([]EventKey, bool, error) {
	h := &eventKeyMinHeap{}
	heap.Init(h)

	var hasToken bool
	var pageTokenKey EventKey
	if len(mq.PageToken) > 0 {
		eventKey, err := DecodePageToken(mq.PageToken)
		if err != nil {
			return nil, false, err
		}
		pageTokenKey = eventKey
		hasToken = true
	}

	boundedSize := mq.PageSize + 1

	for _, mt := range mts {
		for _, item := range mt.items[:mt.count] {
			ev := item.event
			evStart := ev.StartTime.AsTime()
			evEnd := ev.EndTime.AsTime()

			if !(evStart.Before(mq.EndTime) || evStart.Equal(mq.EndTime)) ||
				!(evEnd.After(mq.StartTime) || evEnd.Equal(mq.StartTime)) {
				continue
			}

			if hasToken {
				if evStart.After(pageTokenKey.StartTime) {
					continue
				}
				if evStart.Equal(pageTokenKey.StartTime) {
					if ev.Id >= pageTokenKey.ID {
						continue
					}
				}
			}

			matched, err := matchMemTableItem(mq.Filter, item)
			if err != nil {
				return nil, false, err
			}

			if matched {
				nextKey := EventKey{
					ResourceTypeCode:   ev.GetResource().GetTypeCode(),
					ResourceExternalID: ev.GetResource().GetExternalId(),
					StartTime:          ev.GetStartTime().AsTime(),
					ID:                 ev.GetId(),
				}
				if h.Len() < boundedSize {
					heap.Push(h, nextKey)
				} else {
					if compareEventKey(nextKey, (*h)[0]) > 0 {
						heap.Pop(h)
						heap.Push(h, nextKey)
					}
				}
			}
		}
	}

	hasNext := h.Len() > mq.PageSize
	if hasNext {
		heap.Pop(h)
	}

	resultLen := h.Len()
	keys := make([]EventKey, resultLen)

	for i := resultLen - 1; i >= 0; i-- {
		keys[i] = heap.Pop(h).(EventKey)
	}

	return keys, hasNext, nil
}
