package bitstar

import (
	"sort"

	"github.com/RoaringBitmap/roaring/v2"
)

type IndexIterator struct {
	ri      *roaringIndex
	posting *roaring.Bitmap
	it      roaring.IntPeekable
	index   uint32

	nextIdx uint32
	hasNext bool

	lastEmittedID string
	hasEmitted    bool

	end bool
	ts  uint64
}

func NewIndexIterator(ri *roaringIndex, posting *roaring.Bitmap, ts uint64) *IndexIterator {
	ii := &IndexIterator{
		ri:      ri,
		posting: posting,
		it:      posting.Iterator(),
		ts:      ts,
	}
	ii.advance()
	return ii
}

func (ii *IndexIterator) Valid() bool {
	return !ii.end
}

func (ii *IndexIterator) Next() bool {
	if ii.end {
		return false
	}

	if !ii.hasNext {
		ii.end = true
		return false
	}

	ii.index = ii.nextIdx
	ii.lastEmittedID = ii.ri.keys[ii.index].ID
	ii.hasEmitted = true

	ii.advance()

	return true
}

func (ii *IndexIterator) Value() EventKey {
	return ii.ri.keys[ii.index]
}

func (ii *IndexIterator) Seek(key EventKey) {
	ii.end = false
	ii.hasNext = false
	ii.hasEmitted = false

	targetIdx := sort.Search(len(ii.ri.keys), func(i int) bool {
		return compareEventKeyTimestamp(ii.ri.keys[i], key) >= 0
	})

	if targetIdx >= len(ii.ri.keys) {
		ii.end = true
		return
	}

	ii.it = ii.posting.Iterator()

	ii.it.AdvanceIfNeeded(uint32(targetIdx))

	ii.advance()
}

func (ii *IndexIterator) advance() {
	ii.hasNext = false

	for ii.it.HasNext() {
		idx := ii.it.Next()
		key := ii.ri.keys[idx]

		if key.Timestamp > ii.ts {
			continue
		}

		if ii.hasEmitted && ii.lastEmittedID == key.ID && key.Timestamp <= ii.ts {
			continue
		}

		ii.nextIdx = idx
		ii.hasNext = true
		return
	}
}
