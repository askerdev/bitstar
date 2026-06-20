package bitstar

import (
	"github.com/RoaringBitmap/roaring/v2"
)

type IndexIterator struct {
	ri    *roaringIndex
	it    roaring.IntPeekable
	index uint32
	end   bool
	ts    uint64
}

func NewIndexIterator(ri *roaringIndex, posting *roaring.Bitmap, ts uint64) *IndexIterator {
	it := posting.Iterator()
	return &IndexIterator{
		ri: ri,
		it: it,
		ts: ts,
	}
}

func (ii *IndexIterator) Valid() bool {
	return !ii.end
}

func (ii *IndexIterator) Next() bool {
	if ii.end {
		return false
	}
	for ii.it.HasNext() && ii.ri.keys[ii.it.PeekNext()].Timestamp > ii.ts {
		ii.it.Next()
	}
	found := ii.it.HasNext()
	if !found {
		ii.end = true
		return false
	}
	ii.index = ii.it.Next()
	return true
}

func (ii *IndexIterator) Value() EventKey {
	return ii.ri.keys[ii.index]
}

func (ii *IndexIterator) Seek(key EventKey) {
	ii.end = false
	if index, ok := ii.ri.indexes[key]; ok {
		ii.it.AdvanceIfNeeded(index)
		ii.index = index
	} else if index, ok := ii.ri.nextMax(key); ok {
		ii.it.AdvanceIfNeeded(index)
		ii.index = index
	} else {
		ii.end = true
	}
}
