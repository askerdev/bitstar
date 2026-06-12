package bitstar

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/askerdev/bitstar/filtering"
	storagepb "github.com/askerdev/bitstar/proto/infralenta/storage/v1"
	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestFromBbolt(t *testing.T) {
	bucketName := []byte("events")

	now := time.Now().Truncate(time.Second)

	id1, id2 := uuid.Must(uuid.NewV7()).String(), uuid.Must(uuid.NewV7()).String()
	startTime1, startTime2 := now, now.Add(-time.Hour)

	events := []*storagepb.Event{
		{
			Id:          id1,
			StartTime:   timestamppb.New(startTime1),
			EndTime:     timestamppb.New(now.Add(time.Hour)),
			Tags:        []string{"go", "backend"},
			Annotations: map[string]string{"env": "prod"},
		},
		{
			Id:          id2,
			StartTime:   timestamppb.New(startTime2),
			EndTime:     timestamppb.New(now),
			Tags:        []string{"go"},
			Annotations: map[string]string{"env": "dev"},
		},
	}

	db := db(t, bucketName, events)

	err := db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketName)
		i := 0
		return b.ForEach(func(k, v []byte) error {
			got := &storagepb.Event{}
			if err := proto.Unmarshal(v, got); err != nil {
				return err
			}
			want := events[len(events)-1-i]
			if diff := cmp.Diff(want, got, protocmp.Transform()); diff != "" {
				t.Fatalf("mismatch (-want +got):\n%s", diff)
			}
			i++
			return nil
		})
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ri, err := fromBbolt(db, bucketName)
	if err != nil {
		t.Fatalf("index from bbolt fail: %v", err)
	}

	if ri.all.GetCardinality() != 2 {
		t.Errorf("expected 2 elements in all, got %d", ri.all.GetCardinality())
	}

	if startTime, id := decodeKey(ri.keys[0]); !startTime.Equal(startTime1) || id.String() != id1 {
		t.Errorf("expected ri.keys[0] to be %q/%q, got %q/%q", startTime1, id1, startTime, id)
	}

	if startTime, id := decodeKey(ri.keys[1]); !startTime.Equal(startTime2) || id.String() != id2 {
		t.Errorf("expected ri.keys[1] to be %q/%q, got %q/%q", startTime2, id2, startTime, id)
	}

	if !ri.tags["go"].Contains(0) || !ri.tags["go"].Contains(1) {
		t.Errorf("expected both to be in ri.tags[go]")
	}

	if !ri.tags["backend"].Contains(0) || ri.tags["backend"].Contains(1) {
		t.Errorf("expected only %q to be in ri.tags[%q]", id1, "backend")
	}

	envProd, envDev := Pair{Key: "env", Value: "prod"}, Pair{Key: "env", Value: "dev"}

	if !ri.annotations[envProd].Contains(0) || ri.annotations[envProd].Contains(1) {
		t.Errorf("expected only %q to be in ri.annotations[%q/%q]", id1, envProd.Key, envProd.Value)
	}

	if !ri.annotations[envDev].Contains(1) || ri.annotations[envDev].Contains(0) {
		t.Errorf("expected only %q to be in ri.annotations[%q/%q]", id2, envDev.Key, envDev.Value)
	}
}

func TestRoaringIndex_Filter(t *testing.T) {
	bucketName := []byte("events")

	now := time.Now().Truncate(time.Second)

	events := []*storagepb.Event{
		{
			Id:          uuid.Must(uuid.NewV7()).String(),
			StartTime:   timestamppb.New(now),
			EndTime:     timestamppb.New(now.Add(time.Hour)),
			Tags:        []string{"go", "backend"},
			Annotations: map[string]string{"env": "prod"},
		},
		{
			Id:          uuid.Must(uuid.NewV7()).String(),
			StartTime:   timestamppb.New(now.Add(-time.Hour)),
			EndTime:     timestamppb.New(now),
			Tags:        []string{"go"},
			Annotations: map[string]string{"env": "dev"},
		},
	}

	db := db(t, bucketName, events)

	ri, err := fromBbolt(db, bucketName)
	if err != nil {
		t.Fatalf("index from bbolt fail: %v", err)
	}

	tc := []struct {
		startTime  time.Time
		endTime    time.Time
		filter     string
		wantEvents []*storagepb.Event
	}{
		{
			startTime:  now,
			endTime:    now,
			filter:     `tags = "go"`,
			wantEvents: events,
		},
		{
			startTime:  now,
			endTime:    now,
			filter:     `tags = "backend"`,
			wantEvents: []*storagepb.Event{events[0]},
		},
		{
			startTime:  now,
			endTime:    now,
			filter:     `annotations.env = "prod"`,
			wantEvents: []*storagepb.Event{events[0]},
		},
		{
			startTime:  now,
			endTime:    now,
			filter:     `annotations.env = "dev"`,
			wantEvents: []*storagepb.Event{events[1]},
		},
		{
			startTime:  now,
			endTime:    now,
			filter:     ``,
			wantEvents: events,
		},
		{
			startTime:  now.Add(-time.Hour),
			endTime:    now.Add(-time.Minute),
			filter:     ``,
			wantEvents: []*storagepb.Event{events[1]},
		},
		{
			startTime:  now.Add(time.Minute),
			endTime:    now.Add(time.Hour),
			filter:     ``,
			wantEvents: []*storagepb.Event{events[0]},
		},
	}

	for _, tt := range tc {
		t.Run(fmt.Sprintf("%q %q %q", tt.startTime, tt.endTime, tt.filter), func(t *testing.T) {
			filter, err := filtering.ParseFilter(tt.filter)
			if err != nil {
				t.Fatalf("ParseFilter fail: %v", err)
			}

			posting, err := ri.query(tt.startTime, tt.endTime, filter)
			if err != nil {
				t.Fatalf("query fail: %v", err)
			}

			wantPosting := roaring.New()
			for _, event := range tt.wantEvents {
				key := encodeKey(event.StartTime.AsTime(), uuid.MustParse(event.GetId()))
				wantPosting.Add(ri.indexes[string(key)])
			}

			if !wantPosting.Equals(posting) {
				t.Errorf("expected %s, got %s", posting, wantPosting)
			}
		})
	}
}

func db(t testing.TB, bucketName []byte, events []*storagepb.Event) *bbolt.DB {
	t.Helper()
	dir := t.TempDir()
	db, err := bbolt.Open(filepath.Join(dir, "bbolt.db"), 0600, nil)
	if err != nil {
		t.Fatalf("bbolt open fail: %v", err)
	}
	err = db.Batch(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(bucketName)
		if err != nil {
			return err
		}
		for _, event := range events {
			key := encodeKey(
				event.GetStartTime().AsTime(),
				uuid.Must(uuid.Parse(event.GetId())),
			)
			val, err := proto.Marshal(event)
			if err != nil {
				return err
			}
			if err := b.Put(key, val); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("bbolt batch fail: %v", err)
	}
	return db
}
