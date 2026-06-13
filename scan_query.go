package bitstar

import (
	"bytes"
	"encoding/base64"
	"slices"
	"time"

	"github.com/askerdev/bitstar/filtering"
	storagepb "github.com/askerdev/bitstar/proto/infralenta/storage/v1"
	"github.com/dgraph-io/badger/v4/skl"
	"google.golang.org/protobuf/proto"
)

type ScanQuery struct {
	PageSize  int
	PageToken string
	StartTime time.Time
	EndTime   time.Time
	Filter    *filtering.Filter
}

func (sq *ScanQuery) Do(skls []*skl.Skiplist) ([][]byte, bool, error) {
	var allKeys [][]byte

	for _, list := range skls {
		if list == nil {
			continue
		}
		it := list.NewIterator()

		for it.SeekToLast(); it.Valid(); it.Prev() {
			key := it.Key()

			event := &storagepb.Event{}
			if err := proto.Unmarshal(it.Value().Value, event); err != nil {
				it.Close()
				return nil, false, err
			}

			if event.GetEndTime().AsTime().Before(sq.StartTime) {
				continue
			}

			if event.GetStartTime().AsTime().After(sq.EndTime) {
				continue
			}

			match, err := matchEvent(sq.Filter, event)
			if err != nil {
				it.Close()
				return nil, false, err
			}

			if match {
				allKeys = append(allKeys, append([]byte(nil), key...))
			}
		}
		it.Close()
	}

	slices.SortFunc(allKeys, func(a, b []byte) int {
		return bytes.Compare(b, a)
	})

	startIndex := 0
	if len(sq.PageToken) > 0 {
		tokenBytes, err := base64.StdEncoding.DecodeString(sq.PageToken)
		if err != nil {
			return nil, false, err
		}

		found := false
		for i, k := range allKeys {
			if bytes.Equal(k, tokenBytes) {
				startIndex = i + 1
				found = true
				break
			}
		}

		if !found {
			startIndex = 0
		}
	}

	endIndex := startIndex + sq.PageSize
	hasNext := false

	if endIndex < len(allKeys) {
		hasNext = true
	} else {
		endIndex = len(allKeys)
	}

	if startIndex >= len(allKeys) {
		return [][]byte{}, false, nil
	}

	return allKeys[startIndex:endIndex], hasNext, nil
}
