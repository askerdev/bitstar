package bitstar

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/askerdev/bitstar/bsi"
	"github.com/askerdev/bitstar/filtering"
	storagepb "github.com/askerdev/bitstar/proto/infralenta/storage/v1"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"google.golang.org/protobuf/proto"
)

type EventKey struct {
	ResourceTypeCode   string
	ResourceExternalID string
	StartTime          time.Time
	ID                 string
	Timestamp          uint64
	IsDeleted          bool
}

func EventKeyFromProto(event *storagepb.Event, ts uint64, isDeleted bool) EventKey {
	return EventKey{
		ResourceTypeCode:   event.GetResource().GetTypeCode(),
		ResourceExternalID: event.GetResource().GetExternalId(),
		StartTime:          event.GetStartTime().AsTime(),
		ID:                 event.GetId(),
		Timestamp:          ts,
		IsDeleted:          isDeleted,
	}
}

type roaringIndex struct {
	all         *roaring.Bitmap
	startTime   *bsi.BSI
	endTime     *bsi.BSI
	tags        map[string]*roaring.Bitmap
	annotations map[Pair]*roaring.Bitmap

	keys    []EventKey
	indexes map[EventKey]uint32
}

func newRoaringIndex() *roaringIndex {
	return &roaringIndex{
		all:         roaring.New(),
		startTime:   bsi.NewDefaultBSI(),
		endTime:     bsi.NewDefaultBSI(),
		tags:        make(map[string]*roaring.Bitmap),
		annotations: make(map[Pair]*roaring.Bitmap),
		indexes:     make(map[EventKey]uint32),
	}
}

func (ri *roaringIndex) query(
	startTime time.Time,
	endTime time.Time,
	filter *filtering.Filter,
) (*roaring.Bitmap, error) {
	res, err := ri.evalFilter(filter)
	if err != nil {
		return nil, err
	}
	res = ri.startTime.CompareValue(4, bsi.LE, endTime.Unix(), 0, res)
	res = ri.endTime.CompareValue(4, bsi.GE, startTime.Unix(), 0, res)
	return res, nil
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

// fromYt loads all events from tablePath into a roaringIndex.
// Rows are returned by ReadTable in primary key order
// (resource_type_code, resource_external_id, start_time, id).
func fromYt(ctx context.Context, ytc yt.Client, tablePath string) (*roaringIndex, error) {
	ri := newRoaringIndex()

	r, err := ytc.ReadTable(ctx, ypath.Path(tablePath), nil)
	if err != nil {
		return nil, fmt.Errorf("read table: %w", err)
	}
	defer r.Close()

	for r.Next() {
		var row EventRow
		if err := r.Scan(&row); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}

		event := &storagepb.Event{}
		if err := proto.Unmarshal(row.Event, event); err != nil {
			return nil, fmt.Errorf("unmarshal proto: %w", err)
		}

		indexEvent(ri, event, 0, false)
	}

	return ri, r.Err()
}

func fromMemTable(mt *memTable) *roaringIndex {
	ri := newRoaringIndex()

	// for elem := mt.items.Front(); elem != nil; elem = elem.Next() {
	// 	key := elem.Key().(EventKey)
	// 	val := elem.Value.(*memTableItem)
	// }

	// slices.SortFunc(mt.items, func(a, b *memTableItem) int {
	// 	timeA := a.event.StartTime.AsTime()
	// 	timeB := b.event.StartTime.AsTime()
	// 	if timeA.After(timeB) {
	// 		return -1
	// 	}
	// 	if timeA.Before(timeB) {
	// 		return 1
	// 	}
	// 	if b.event.Id > a.event.Id {
	// 		return 1
	// 	}
	// 	if b.event.Id < a.event.Id {
	// 		return -1
	// 	}
	// 	if a.ts < b.ts {
	// 		return -1
	// 	}
	// 	if a.ts > b.ts {
	// 		return 1
	// 	}
	// 	return 0
	// })

	// for i, item := range mt.items {
	// 	if i < len(mt.items)-1 && item.event.Id == mt.items[i+1].event.Id {
	// 		continue
	// 	}
	// 	indexEvent(ri, item.event, item.ts, item.isDeleted)
	// }

	return ri
}

func mergeTwoIndices(a, b *roaringIndex) *roaringIndex {
	if a == nil {
		return b
	}

	if b == nil {
		return a
	}

	ri := &roaringIndex{
		all:         roaring.New(),
		startTime:   bsi.NewDefaultBSI(),
		endTime:     bsi.NewDefaultBSI(),
		tags:        make(map[string]*roaring.Bitmap),
		annotations: make(map[Pair]*roaring.Bitmap),
		indexes:     make(map[EventKey]uint32),
		keys:        make([]EventKey, 0, len(a.keys)+len(b.keys)),
	}

	remapA := make([]uint32, len(a.keys))
	remapB := make([]uint32, len(b.keys))

	i, j := 0, 0
	for i < len(a.keys) && j < len(b.keys) {
		takeA := false

		tCmp := a.keys[i].StartTime.Compare(b.keys[j].StartTime)
		if tCmp != 0 {
			takeA = tCmp < 0
		} else {
			takeA = a.keys[i].ID >= b.keys[j].ID
		}

		if takeA {
			ri.keys = append(ri.keys, a.keys[i])
			newRID := uint32(len(ri.keys) - 1)
			remapA[i] = newRID
			i++
		} else {
			ri.keys = append(ri.keys, b.keys[j])
			newRID := uint32(len(ri.keys) - 1)
			remapB[j] = newRID
			j++
		}
	}

	for i < len(a.keys) {
		ri.keys = append(ri.keys, a.keys[i])
		newRID := uint32(len(ri.keys) - 1)
		remapA[i] = newRID
		i++
	}

	for j < len(b.keys) {
		ri.keys = append(ri.keys, b.keys[j])
		newRID := uint32(len(ri.keys) - 1)
		remapB[j] = newRID
		j++
	}

	if len(ri.keys) > 0 {
		ri.all.AddRange(0, uint64(len(ri.keys)))
	}

	for strKey, oldRID := range a.indexes {
		ri.indexes[strKey] = remapA[oldRID]
	}

	for strKey, oldRID := range b.indexes {
		ri.indexes[strKey] = remapB[oldRID]
	}

	for tag, rb := range a.tags {
		if _, ok := ri.tags[tag]; !ok {
			ri.tags[tag] = roaring.New()
		}
		moveBitmap(rb, remapA, ri.tags[tag])
	}

	for tag, rb := range b.tags {
		if _, ok := ri.tags[tag]; !ok {
			ri.tags[tag] = roaring.New()
		}
		moveBitmap(rb, remapB, ri.tags[tag])
	}

	for annotation, rb := range a.annotations {
		if _, ok := ri.annotations[annotation]; !ok {
			ri.annotations[annotation] = roaring.New()
		}
		moveBitmap(rb, remapA, ri.annotations[annotation])
	}

	for annotation, rb := range b.annotations {
		if _, ok := ri.annotations[annotation]; !ok {
			ri.annotations[annotation] = roaring.New()
		}
		moveBitmap(rb, remapB, ri.annotations[annotation])
	}

	totalKeys := uint64(len(ri.keys))
	ri.startTime = mergeBSI(a.startTime, b.startTime, remapA, remapB, totalKeys)
	ri.endTime = mergeBSI(a.endTime, b.endTime, remapA, remapB, totalKeys)

	return ri
}

func moveBitmap(src *roaring.Bitmap, remap []uint32, dst *roaring.Bitmap) {
	card := src.GetCardinality()
	if card == 0 {
		return
	}

	if card < 32 {
		it := src.Iterator()
		for it.HasNext() {
			dst.Add(remap[it.Next()])
		}
		return
	}

	buf := make([]uint32, card)

	it := src.ManyIterator()
	it.NextMany(buf)

	for i := range buf {
		buf[i] = remap[buf[i]]
	}

	dst.AddMany(buf)
}

func mergeBSI(srcA, srcB *bsi.BSI, remapA, remapB []uint32, maxLen uint64) *bsi.BSI {
	maxBitCount := srcA.BitCount()
	if srcB.BitCount() > maxBitCount {
		maxBitCount = srcB.BitCount()
	}

	maxVal := max(srcB.MaxValue, srcA.MaxValue)
	minVal := min(srcB.MinValue, srcA.MinValue)

	dst := bsi.NewBSI(maxVal, minVal)

	dst.InitBitmaps(maxBitCount)

	dstBitmaps := dst.GetBitmaps()

	srcABitmaps := srcA.GetBitmaps()
	srcBBitmaps := srcB.GetBitmaps()

	for bitIdx, rb := range srcABitmaps {
		if bitIdx < len(dstBitmaps) {
			moveBitmap(rb, remapA, dstBitmaps[bitIdx])
		}
	}

	for bitIdx, rb := range srcBBitmaps {
		if bitIdx < len(dstBitmaps) {
			moveBitmap(rb, remapB, dstBitmaps[bitIdx])
		}
	}

	if maxLen > 0 {
		dst.GetExistenceBitmap().AddRange(0, maxLen)
	}

	return dst
}

func indexEvent(ri *roaringIndex, event *storagepb.Event, ts uint64, isDeleted bool) {
	key := EventKey{
		ResourceTypeCode:   event.GetResource().GetTypeCode(),
		ResourceExternalID: event.GetResource().GetExternalId(),
		StartTime:          event.GetStartTime().AsTime().Local(),
		ID:                 event.GetId(),
		Timestamp:          ts,
		IsDeleted:          isDeleted,
	}
	ri.keys = append(ri.keys, key)
	index := uint32(len(ri.keys) - 1)
	ri.all.Add(index)
	ri.indexes[key] = index

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
