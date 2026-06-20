package bitstar

import (
	"github.com/askerdev/bitstar/filtering"
	"github.com/huandu/skiplist"
)

type MemTableIterator struct {
	mt   *memTable
	ts   uint64
	f    *filtering.Filter
	cur  *skiplist.Element
	next *skiplist.Element
	end  bool
}

func NewMemTableIterator(mt *memTable, f *filtering.Filter, ts uint64) *MemTableIterator {
	return &MemTableIterator{
		mt: mt,
		f:  f,
		ts: ts,
	}
}

func (mti *MemTableIterator) Valid() bool {
	return !mti.end
}

func (mti *MemTableIterator) Next() bool {
	if mti.end {
		return false
	}

	mti.mt.mu.RLock()
	defer mti.mt.mu.RUnlock()

	if mti.cur == nil && mti.next == nil {
		mti.next = mti.mt.items.Front()
	}

	if mti.next == nil {
		mti.end = true
		return false
	}

	cur := mti.next
	for ; cur != nil; cur = cur.Next() {
		prev := cur.Prev()
		key := cur.Key().(EventKey)

		if key.Timestamp > mti.ts {
			continue
		}

		if prev != nil && key.ID == prev.Key().(EventKey).ID && prev.Key().(EventKey).Timestamp <= mti.ts {
			continue
		}

		match, err := matchMemTableItem(mti.f, cur.Value.(*memTableItem))
		if !match || err != nil {
			continue
		}

		break
	}
	mti.cur = cur
	if mti.cur == nil {
		mti.end = true
		mti.next = nil
	} else {
		mti.next = mti.cur.Next()
	}
	return mti.cur != nil
}

func (mti *MemTableIterator) Value() EventKey {
	mti.mt.mu.RLock()
	defer mti.mt.mu.RUnlock()
	return mti.cur.Key().(EventKey)
}

func (mti *MemTableIterator) Seek(key EventKey) {
	mti.end = false

	mti.mt.mu.RLock()
	defer mti.mt.mu.RUnlock()

	found := mti.mt.items.Find(key)
	if found == nil {
		mti.end = true
		return
	}
	mti.next = found
}
