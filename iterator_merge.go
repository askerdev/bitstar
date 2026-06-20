package bitstar

type MergeIterator struct {
	init       bool
	iterators  []Iterator
	currentKey EventKey
	end        bool
}

func NewMergeIterator(iterators []Iterator) *MergeIterator {
	return &MergeIterator{
		init:      true,
		iterators: iterators,
	}
}

func (mi *MergeIterator) Valid() bool {
	return !mi.end
}

func (mi *MergeIterator) Next() bool {
	if mi.end {
		return false
	}

	if mi.init {
		for _, it := range mi.iterators {
			it.Next()
		}
		mi.init = false
	}

	var minValue EventKey
	minIndex := -1
	for i, it := range mi.iterators {
		if !it.Valid() {
			continue
		}

		key := it.Value()

		if minIndex == -1 {
			minValue = key
			minIndex = i
			continue
		}

		cmp := compareEventKey(key, minValue)
		if cmp == 1 {
			minValue = key
			minIndex = i
			continue
		}

		if cmp == -1 {
			continue
		}

		if key.Timestamp > minValue.Timestamp {
			minValue = key
			minIndex = i
		} else {
			it.Next()
		}
	}

	mi.currentKey = minValue

	if minIndex == -1 {
		mi.end = true
	} else {
		mi.iterators[minIndex].Next()
	}

	return minIndex != -1
}

func (mi *MergeIterator) Value() EventKey {
	return mi.currentKey
}

func (mi *MergeIterator) Seek(key EventKey) {
	for _, it := range mi.iterators {
		it.Seek(key)
	}
}
