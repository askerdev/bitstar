package bitstar

import (
	"sync"

	storagepb "github.com/askerdev/bitstar/proto/infralenta/storage/v1"
	"github.com/bits-and-blooms/bloom/v3"
	"github.com/huandu/skiplist"
)

type memTableItem struct {
	event       *storagepb.Event
	tags        *bloom.BloomFilter
	annotations *bloom.BloomFilter
	ts          uint64
	isDeleted   bool
}

func newMemTableItem(event *storagepb.Event, ts uint64, isDeleted bool) *memTableItem {
	item := &memTableItem{
		tags:        bloom.NewWithEstimates(50, 0.01),
		annotations: bloom.NewWithEstimates(50, 0.01),
		ts:          ts,
		isDeleted:   isDeleted,
	}
	if event != nil {
		item.event = event
		for _, tag := range event.Tags {
			item.tags.AddString(tag)
		}
		for k, v := range event.Annotations {
			item.annotations.AddString(k + "_" + v)
		}
	}
	return item
}

type memTable struct {
	items *skiplist.SkipList
	mu    sync.RWMutex
	wg    sync.WaitGroup
}

func newMemTable() *memTable {
	return &memTable{
		items: skiplist.New(skiplist.LessThanFunc(func(lhs, rhs any) int {
			a, b := lhs.(EventKey), rhs.(EventKey)

			if a.StartTime.After(b.StartTime) {
				return 1
			}
			if a.StartTime.Before(b.StartTime) {
				return -1
			}

			if a.ID > b.ID {
				return 1
			}

			if a.ID < b.ID {
				return -1
			}

			if a.Timestamp < b.Timestamp {
				return -1
			}

			if a.Timestamp > b.Timestamp {
				return 1
			}

			return 0
		})),
	}
}

func (mt *memTable) size() int {
	return mt.items.Len()
}

func (mt *memTable) putItem(key EventKey, mti *memTableItem) {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	mt.items.Set(key, mti)
}

func (mt *memTable) put(event *storagepb.Event, ts uint64, isDeleted bool) {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	mt.items.Set(
		EventKeyFromProto(event, ts, isDeleted),
		newMemTableItem(event, ts, isDeleted),
	)
}
