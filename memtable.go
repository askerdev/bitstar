package bitstar

import (
	"sync"
	"sync/atomic"

	storagepb "github.com/askerdev/bitstar/proto/infralenta/storage/v1"
	"github.com/bits-and-blooms/bloom/v3"
)

type memTableItem struct {
	event       *storagepb.Event
	tags        *bloom.BloomFilter
	annotations *bloom.BloomFilter
}

type memTable struct {
	items []*memTableItem
	count atomic.Uint32
	mu    sync.RWMutex
	wg    sync.WaitGroup
}

func newMemTable() *memTable {
	return &memTable{}
}

func (mt *memTable) size() uint32 {
	return mt.count.Load()
}

func (mt *memTable) put(event *storagepb.Event) {
	item := &memTableItem{
		event:       event,
		tags:        bloom.NewWithEstimates(50, 0.01),
		annotations: bloom.NewWithEstimates(50, 0.01),
	}
	for _, tag := range event.Tags {
		item.tags.AddString(tag)
	}
	for k, v := range event.Annotations {
		item.annotations.AddString(k + "_" + v)
	}
	mt.items = append(mt.items, item)
	mt.count.Add(1)
}
