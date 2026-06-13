package bitstar

import (
	"sync"

	"github.com/dgraph-io/badger/v4/skl"
)

type MemTable struct {
	skl *skl.Skiplist
	wg  sync.WaitGroup
}
