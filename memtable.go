package bitstar

import (
	"sync"

	storagepb "github.com/askerdev/bitstar/proto/infralenta/storage/v1"
	"github.com/bits-and-blooms/bloom/v3"
)

type memTableItem struct {
	event       *storagepb.Event
	tags        *bloom.BloomFilter
	annotations *bloom.BloomFilter
	ts          uint64
}

func newMemTableItem(event *storagepb.Event, ts uint64) *memTableItem {
	item := &memTableItem{
		event:       event,
		tags:        bloom.NewWithEstimates(50, 0.01),
		annotations: bloom.NewWithEstimates(50, 0.01),
		ts:          ts,
	}
	for _, tag := range event.Tags {
		item.tags.AddString(tag)
	}
	for k, v := range event.Annotations {
		item.annotations.AddString(k + "_" + v)
	}
	return item
}

type memTable struct {
	items []*memTableItem
	count int
	wg    sync.WaitGroup
}

func newMemTable() *memTable {
	return &memTable{}
}

func (mt *memTable) size() int {
	return mt.count
}

func (mt *memTable) putItem(mti *memTableItem) {
	mt.items = append(mt.items, mti)
	mt.count++
}

func (mt *memTable) put(event *storagepb.Event, ts uint64) {
	mt.items = append(mt.items, newMemTableItem(event, ts))
	mt.count++
}
