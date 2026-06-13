package bitstar

import (
	"bytes"
	"encoding/base64"
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

func (iq *IndexQuery) Do(ris []*roaringIndex) ([][]byte, bool, error) {
	keys := make([][]byte, 0, iq.PageSize)

	var pageTokenKey []byte
	if len(iq.PageToken) > 0 {
		buf, err := base64.StdEncoding.DecodeString(iq.PageToken)
		if err != nil {
			return nil, false, err
		}
		pageTokenKey = buf
	}

	hasNext := false
	for _, ri := range ris {
		if ri == nil {
			continue
		}

		posting, err := ri.query(iq.StartTime, iq.EndTime, iq.Filter)
		if err != nil {
			return nil, false, err
		}

		it := posting.Iterator()
		if len(pageTokenKey) > 0 {
			if index, ok := ri.indexes[string(pageTokenKey)]; ok {
				it.AdvanceIfNeeded(index + 1)
			} else if index, ok := ri.nextMax(pageTokenKey); ok {
				it.AdvanceIfNeeded(index)
			} else {
				continue
			}
		}

		page := make([][]byte, 0, iq.PageSize)

		for it.HasNext() && len(page) < int(iq.PageSize) {
			nextKey := ri.keys[it.Next()]
			page = append(page, nextKey)
		}

		currentTotalLen := len(keys) + len(page)

		keys = mergeKeysLimited(keys, page, iq.PageSize)

		if currentTotalLen > len(keys) || it.HasNext() {
			hasNext = true
		}
	}

	return keys, hasNext, nil
}

func mergeKeysLimited(a, b [][]byte, pageSize int) [][]byte {
	c := make([][]byte, 0, pageSize)

	i, j := 0, 0
	for i < len(a) && j < len(b) && len(c) < pageSize {
		compare := bytes.Compare(a[i], b[j])
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

func mergeKeys(a, b [][]byte) [][]byte {
	c := make([][]byte, 0, len(a)+len(b))

	i, j := 0, 0
	for i < len(a) && j < len(b) {
		compare := bytes.Compare(a[i], b[j])
		if compare >= 0 {
			c = append(c, a[i])
			i++
		} else {
			c = append(c, b[j])
			j++
		}
	}

	for i < len(a) {
		c = append(c, a[i])
		i++
	}

	for j < len(b) {
		c = append(c, b[j])
		j++
	}

	return c
}
