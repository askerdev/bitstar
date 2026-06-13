package bitstar

import (
	"container/heap"
	"encoding/base64"
	"time"

	"github.com/askerdev/bitstar/filtering"
	"github.com/google/uuid"
)

type MemTableQuery struct {
	PageSize  int
	PageToken string
	StartTime time.Time
	EndTime   time.Time
	Filter    *filtering.Filter
}

func (mq *MemTableQuery) Do(mts []*memTable) ([][]byte, bool, error) {
	h := &eventMinHeap{}
	heap.Init(h)

	var tokenTime time.Time
	var tokenID string
	var hasToken bool

	if len(mq.PageToken) > 0 {
		buf, err := base64.StdEncoding.DecodeString(mq.PageToken)
		if err != nil {
			return nil, false, err
		}
		var uuidObj uuid.UUID
		tokenTime, uuidObj = decodeKey(buf)
		tokenID = uuidObj.String()
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
				if evStart.After(tokenTime) {
					continue
				}
				if evStart.Equal(tokenTime) {
					if ev.Id >= tokenID {
						continue
					}
				}
			}

			matched, err := matchMemTableItem(mq.Filter, item)
			if err != nil {
				return nil, false, err
			}

			if matched {
				if h.Len() < boundedSize {
					heap.Push(h, &heapItem{item: item})
				} else {
					if isNewer(item, (*h)[0].item) {
						heap.Pop(h)
						heap.Push(h, &heapItem{item: item})
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
	keys := make([][]byte, resultLen)

	for i := resultLen - 1; i >= 0; i-- {
		curr := heap.Pop(h).(*heapItem)
		ev := curr.item.event
		keys[i] = encodeKey(ev.StartTime.AsTime(), uuid.MustParse(ev.Id))
	}

	return keys, hasNext, nil
}
