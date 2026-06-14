package bitstar

import (
	"time"

	"github.com/askerdev/bitstar/filtering"
)

type IndexQuery struct {
	PageSize  int
	PageToken string
	StartTime time.Time
	EndTime   time.Time
	Filter    *filtering.Filter
}

func (iq *IndexQuery) Do(ris []*roaringIndex) ([]EventKey, bool, error) {
	var hasToken bool
	var pageTokenKey EventKey
	if len(iq.PageToken) > 0 {
		eventKey, err := DecodePageToken(iq.PageToken)
		if err != nil {
			return nil, false, err
		}
		pageTokenKey = eventKey
		hasToken = true
	}

	boundedSize := iq.PageSize + 1
	keys := make([]EventKey, 0, boundedSize)

	for _, ri := range ris {
		if ri == nil {
			continue
		}

		posting, err := ri.query(iq.StartTime, iq.EndTime, iq.Filter)
		if err != nil {
			return nil, false, err
		}

		it := posting.Iterator()
		if hasToken {
			if index, ok := ri.indexes[pageTokenKey]; ok {
				it.AdvanceIfNeeded(index + 1)
			} else if index, ok := ri.nextMax(pageTokenKey); ok {
				it.AdvanceIfNeeded(index)
			} else {
				continue
			}
		}

		page := make([]EventKey, 0, boundedSize)
		for it.HasNext() && len(page) < boundedSize {
			nextKey := ri.keys[it.Next()]
			page = append(page, nextKey)
		}

		keys = mergeKeysLimited(keys, page, boundedSize)
	}

	hasNext := len(keys) > iq.PageSize
	if hasNext {
		keys = keys[:len(keys)-1]
	}

	return keys, hasNext, nil
}

func mergeKeysLimited(a, b []EventKey, pageSize int) []EventKey {
	c := make([]EventKey, 0, pageSize)

	i, j := 0, 0
	for i < len(a) && j < len(b) && len(c) < pageSize {
		compare := compareEventKey(a[i], b[j])
		if compare >= 0 {
			c = append(c, a[i])
			i++
		} else {
			c = append(c, b[j])
			j++
		}
	}

	for i < len(a) && len(c) < pageSize {
		c = append(c, a[i])
		i++
	}

	for j < len(b) && len(c) < pageSize {
		c = append(c, b[j])
		j++
	}

	return c
}
