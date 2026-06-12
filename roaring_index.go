package bitstar

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/askerdev/bitstar/bsi"
	"github.com/askerdev/bitstar/filtering"
	storagepb "github.com/askerdev/bitstar/proto/infralenta/storage/v1"
	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/skl"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type roaringIndex struct {
	log         *zap.SugaredLogger
	all         *roaring.Bitmap
	startTime   *bsi.BSI
	endTime     *bsi.BSI
	tags        map[string]*roaring.Bitmap
	annotations map[Pair]*roaring.Bitmap
	keys        [][]byte
	indexes     map[string]uint32
}

func (ri *roaringIndex) query(ctx context.Context, in *storagepb.ListEventsRequest) ([][]byte, bool, error) {
	var res *roaring.Bitmap

	err := traceErr(ri.log, "filter mark", func() error {
		filter, err := filtering.ParseFilter(in.GetFilter())
		if err != nil {
			return err
		}
		res, err = ri.evalFilter(filter)
		return err
	})
	if err != nil {
		return nil, false, err
	}

	trace(ri.log, "filter range start_time", func() {
		res = ri.startTime.CompareValue(4, bsi.LE, in.GetEndTime().AsTime().Unix(), 0, res)
	})

	trace(ri.log, "filter range end_time", func() {
		res = ri.endTime.CompareValue(4, bsi.GE, in.GetStartTime().AsTime().Unix(), 0, res)
	})

	iter := res.Iterator()
	if len(in.GetPageToken()) > 0 {
		maxLen := base64.StdEncoding.DecodedLen(len(in.GetPageToken()))
		buf := make([]byte, maxLen)
		if _, err := base64.StdEncoding.Decode(buf, []byte(in.GetPageToken())); err != nil {
			panic(err)
		}
		iter.AdvanceIfNeeded(ri.indexes[string(buf)])
	}

	events := make([]*storagepb.Event, 0, in.GetPageSize())

	keys := make([][]byte, 0, in.GetPageSize())

	err = traceErr(ri.log, "collect result", func() error {
		for iter.HasNext() && len(events) < int(in.GetPageSize()) {
			keys = append(keys, ri.keys[iter.Next()])
		}
		return nil
	})
	if err != nil {
		return nil, false, err
	}

	return keys, iter.HasNext(), nil
}

func (ri *roaringIndex) evalFilter(filter *filtering.Filter) (*roaring.Bitmap, error) {
	if filter == nil || filter.Expression == nil {
		return nil, nil
	}
	var res *roaring.Bitmap
	for _, seq := range filter.Expression.Sequences {
		bitmap, err := ri.evalSequence(seq)
		if err != nil {
			return nil, err
		}
		if res == nil {
			res = bitmap
		} else if bitmap != nil {
			res.And(bitmap)
		}
	}
	return res, nil
}

func (ri *roaringIndex) evalSequence(sequence *filtering.Sequence) (*roaring.Bitmap, error) {
	if sequence == nil {
		return nil, nil
	}
	var res *roaring.Bitmap
	for _, factor := range sequence.Factors {
		bitmap, err := ri.evalFactor(factor)
		if err != nil {
			return nil, err
		}
		if res == nil {
			res = bitmap
		} else if bitmap != nil {
			res.And(bitmap)
		}
	}
	return res, nil
}

func (ri *roaringIndex) evalFactor(factor *filtering.Factor) (*roaring.Bitmap, error) {
	if factor == nil {
		return nil, nil
	}
	var res *roaring.Bitmap
	for _, term := range factor.Terms {
		bitmap, err := ri.evalTerm(term)
		if err != nil {
			return nil, err
		}
		if res == nil {
			res = bitmap
		} else if bitmap != nil {
			res.Or(bitmap)
		}
	}
	return res, nil
}

func (ri *roaringIndex) evalTerm(term *filtering.Term) (*roaring.Bitmap, error) {
	if term == nil {
		return nil, nil
	}
	bitmap, err := ri.evalSimple(term.Simple)
	if err != nil {
		return nil, err
	}
	if bitmap == nil {
		return nil, nil
	}
	if term.Negated {
		allNot := ri.all.Clone()
		allNot.AndNot(bitmap)
		bitmap = allNot
	}
	return bitmap, nil
}

func (ri *roaringIndex) evalSimple(simple *filtering.Simple) (*roaring.Bitmap, error) {
	if simple == nil {
		return nil, nil
	}
	switch {
	case simple.Composite != nil:
		return ri.evalFilter(&filtering.Filter{Expression: simple.Composite})
	case simple.Restriction != nil:
		return ri.evalRestriction(simple.Restriction)
	}
	return nil, nil
}

func (ri *roaringIndex) evalRestriction(r *filtering.Restriction) (*roaring.Bitmap, error) {
	if r.Comparable == nil || r.Comparable.Member == nil || r.Comparable.Member.Value == nil {
		return nil, errors.New("invalid restriction")
	}

	fieldName := r.Comparable.Member.Value.Value
	switch fieldName {
	case "tags":
		if r.Arg == nil || r.Arg.Comparable == nil || r.Arg.Comparable.Member == nil || r.Arg.Comparable.Member.Value == nil {
			return nil, errors.New("`tags` requires argument")
		}
		if !r.Arg.Comparable.Member.Value.Quoted {
			return nil, errors.New("`tags` value type mismatch, expected string, got not quoted value")
		}
		tagName := r.Arg.Comparable.Member.Value.Value
		if rb, ok := ri.tags[tagName]; ok {
			return rb.Clone(), nil
		}
		return roaring.New(), nil
	case "annotations":
		if len(r.Comparable.Member.Fields) == 0 || r.Arg == nil || r.Arg.Comparable == nil || r.Arg.Comparable.Member == nil || r.Arg.Comparable.Member.Value == nil {
			return nil, errors.New("`annotations` requires key and value")
		}
		if !r.Arg.Comparable.Member.Value.Quoted {
			return nil, errors.New("`annotations` value type mismatch, expected string, got not quoted value")
		}
		key := r.Comparable.Member.Fields[0].Value
		value := r.Arg.Comparable.Member.Value.Value
		pair := Pair{Key: key, Value: value}
		if rb, ok := ri.annotations[pair]; ok {
			return rb.Clone(), nil
		}
		return roaring.New(), nil
	default:
		return nil, fmt.Errorf("unknown field %q", fieldName)
	}
}

func fromSkipList(skl *skl.Skiplist) (*roaringIndex, error) {
	ri := &roaringIndex{
		all:         roaring.New(),
		startTime:   bsi.NewDefaultBSI(),
		endTime:     bsi.NewDefaultBSI(),
		tags:        make(map[string]*roaring.Bitmap),
		annotations: make(map[Pair]*roaring.Bitmap),
		indexes:     make(map[string]uint32),
	}

	it := skl.NewIterator()
	defer it.Close()

	for it.SeekToLast(); it.Valid(); it.Prev() {
		event := &storagepb.Event{}
		if err := proto.Unmarshal(it.Value().Value, event); err != nil {
			return nil, err
		}

		key := it.Key()
		ri.keys = append(ri.keys, key)
		index := uint32(len(ri.keys) - 1)
		ri.indexes[string(key)] = index

		ri.all.Add(index)

		ri.startTime.SetValue(uint64(index), event.StartTime.AsTime().Unix())
		ri.endTime.SetValue(uint64(index), event.EndTime.AsTime().Unix())

		for _, tag := range event.Tags {
			if _, ok := ri.tags[tag]; !ok {
				ri.tags[tag] = roaring.New()
			}
			ri.tags[tag].Add(index)
		}

		for key, value := range event.Annotations {
			pair := Pair{Key: key, Value: value}
			if _, ok := ri.annotations[pair]; !ok {
				ri.annotations[pair] = roaring.New()
			}
			ri.annotations[pair].Add(index)
		}
	}

	return ri, nil
}

func fromBadger(kvs *badger.DB) (*roaringIndex, error) {
	ri := &roaringIndex{
		all:         roaring.New(),
		startTime:   bsi.NewDefaultBSI(),
		endTime:     bsi.NewDefaultBSI(),
		tags:        make(map[string]*roaring.Bitmap),
		annotations: make(map[Pair]*roaring.Bitmap),
		indexes:     make(map[string]uint32),
	}

	err := kvs.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{
			PrefetchValues: true,
			PrefetchSize:   100,
			Reverse:        true,
			AllVersions:    false,
		})
		defer it.Close()

		for ; it.Valid(); it.Next() {
			item := it.Item()

			err := item.Value(func(val []byte) error {
				event := &storagepb.Event{}
				if err := proto.Unmarshal(val, event); err != nil {
					return err
				}

				key := item.Key()
				ri.keys = append(ri.keys, key)
				index := uint32(len(ri.keys) - 1)
				ri.indexes[string(key)] = index

				ri.all.Add(index)

				ri.startTime.SetValue(uint64(index), event.StartTime.AsTime().Unix())
				ri.endTime.SetValue(uint64(index), event.EndTime.AsTime().Unix())

				for _, tag := range event.Tags {
					if _, ok := ri.tags[tag]; !ok {
						ri.tags[tag] = roaring.New()
					}
					ri.tags[tag].Add(index)
				}

				for key, value := range event.Annotations {
					pair := Pair{Key: key, Value: value}
					if _, ok := ri.annotations[pair]; !ok {
						ri.annotations[pair] = roaring.New()
					}
					ri.annotations[pair].Add(index)
				}

				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return ri, nil
}
