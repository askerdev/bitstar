package bitstar

import (
	"bytes"
	"encoding/base64"
	"time"

	"github.com/askerdev/bitstar/filtering"
)

type Query struct {
	PageSize  int
	PageToken string
	StartTime time.Time
	EndTime   time.Time
	Filter    string

	lastKey []byte
}

func (q *Query) Do(ris []*roaringIndex) ([][]byte, error) {
	filter, err := filtering.ParseFilter(q.Filter)
	if err != nil {
		return nil, err
	}

	keys := make([][]byte, 0, q.PageSize)

	var pageTokenKey []byte
	if len(q.PageToken) > 0 {
		buf, err := base64.StdEncoding.DecodeString(q.PageToken)
		if err != nil {
			return nil, err
		}
		pageTokenKey = buf
	}

	hasNext := false
	for _, ri := range ris {
		if ri == nil {
			continue
		}

		posting, err := ri.query(q.StartTime, q.EndTime, filter)
		if err != nil {
			return nil, err
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

		page := make([][]byte, 0, q.PageSize)

		for it.HasNext() && len(page) < int(q.PageSize) {
			nextKey := ri.keys[it.Next()]
			page = append(page, nextKey)
		}

		currentTotalLen := len(keys) + len(page)

		keys = mergeKeys(keys, page, q.PageSize)

		if currentTotalLen > len(keys) || it.HasNext() {
			hasNext = true
		}
	}

	if hasNext && len(keys) > 0 {
		q.lastKey = keys[len(keys)-1]
	} else {
		q.lastKey = nil
	}

	return keys, nil
}

func mergeKeys(a, b [][]byte, pageSize int) [][]byte {
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

func (q *Query) NextPageToken() string {
	if len(q.lastKey) == 0 {
		return ""
	}
	return base64.StdEncoding.EncodeToString(q.lastKey)
}
