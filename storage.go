package bitstar

import (
	"context"

	storagepb "github.com/askerdev/bitstar/proto/infralenta/storage/v1"
	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/skl"
)

type Storage struct {
	cur    *skl.Skiplist
	pen    *skl.Skiplist
	kvs    *badger.DB
	levels [4]*roaringIndex
}

func Open(path string) (*Storage, error) {
	kvs, err := badger.Open(badger.DefaultOptions(path))
	if err != nil {
		return nil, err
	}

	s := &Storage{
		cur: skl.NewSkiplist(256 << 20),
		kvs: kvs,
	}

	if err := s.recover(); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *Storage) compact() {
	return
}

func (s *Storage) recover() error {
	ri, err := fromBadger(s.kvs)
	if err != nil {
		return err
	}
	s.levels[3] = ri
	return nil
}

func (s *Storage) BatchCreateEvents(ctx context.Context, in *storagepb.BatchCreateEventsRequest) (*storagepb.BatchCreateEventsResponse, error) {
	return nil, nil
}

func (s *Storage) ListEvents(ctx context.Context, in *storagepb.ListEventsRequest) (*storagepb.ListEventsResponse, error) {
	return nil, nil
}

func (s *Storage) query(ctx context.Context, in *storagepb.ListEventsRequest) ([][]byte, bool, error) {
	totalHasNext := false
	for _, lvl := range s.levels {
		_, _, err := lvl.query(ctx, in)
		if err != nil {
			return nil, false, err
		}
	}
	return nil, totalHasNext, nil
}

func mergeKeys(a, b [][]byte, pageSize int) [][]byte {
	return nil
}
