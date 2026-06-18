package bitstar

import (
	"fmt"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/askerdev/bitstar/filtering"
	storagepb "github.com/askerdev/bitstar/proto/infralenta/storage/v1"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestRoaringIndex(t *testing.T) {
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

	ri := riFromEvents(t, events)

	if ri.all.GetCardinality() != 2 {
		t.Errorf("expected 2 elements in all, got %d", ri.all.GetCardinality())
	}

	if !ri.keys[0].StartTime.Equal(startTime1) || ri.keys[0].ID != id1 {
		t.Errorf("expected ri.keys[0] to be %q/%q, got %q/%q", startTime1, id1, ri.keys[0].StartTime, ri.keys[0].ID)
	}

	if !ri.keys[1].StartTime.Equal(startTime2) || ri.keys[1].ID != id2 {
		t.Errorf("expected ri.keys[1] to be %q/%q, got %q/%q", startTime2, id2, ri.keys[1].StartTime, ri.keys[1].ID)
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

	ri := riFromEvents(t, events)

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
				wantPosting.Add(ri.indexes[EventKey{
					StartTime: event.GetStartTime().AsTime(),
					ID:        event.GetId(),
				}])
			}

			if !wantPosting.Equals(posting) {
				t.Errorf("expected %s, got %s", posting, wantPosting)
			}
		})
	}
}

func TestMergeTwoIndices(t *testing.T) {
	now := time.Now().Truncate(time.Second)

	events0 := []*storagepb.Event{
		{
			Id:          uuid.Must(uuid.NewV7()).String(),
			StartTime:   timestamppb.New(now.Add(time.Minute * 10)),
			EndTime:     timestamppb.New(now.Add(time.Hour)),
			Tags:        []string{"go", "fast"},
			Annotations: map[string]string{"tier": "0"},
		},
		{
			Id:          uuid.Must(uuid.NewV7()).String(),
			StartTime:   timestamppb.New(now.Add(time.Minute * 5)),
			EndTime:     timestamppb.New(now.Add(time.Hour)),
			Tags:        []string{"backend"},
			Annotations: map[string]string{"tier": "0"},
		},
	}

	events1 := []*storagepb.Event{
		{
			Id:          uuid.Must(uuid.NewV7()).String(),
			StartTime:   timestamppb.New(now.Add(-time.Hour)),
			EndTime:     timestamppb.New(now),
			Tags:        []string{"go"},
			Annotations: map[string]string{"tier": "1"},
		},
	}

	ri0 := riFromEvents(t, events0)
	ri1 := riFromEvents(t, events1)

	merged := mergeTwoIndices(ri0, ri1)

	if merged.all.GetCardinality() != 3 {
		t.Errorf("expected 3 elements in total, got %d", merged.all.GetCardinality())
	}

	for i := 0; i < len(merged.keys)-1; i++ {
		if compareEventKey(merged.keys[i], merged.keys[i+1]) <= 0 {
			t.Errorf("sort order violation at index %d: key %v is not greater than %v", i, merged.keys[i], merged.keys[i+1])
		}
	}

	if !merged.tags["fast"].Contains(0) || merged.tags["fast"].Contains(1) || merged.tags["fast"].Contains(2) {
		t.Errorf("invalid mapping for tag 'fast', bitmap: %s", merged.tags["fast"])
	}

	if !merged.tags["go"].Contains(0) || !merged.tags["go"].Contains(2) || merged.tags["go"].Contains(1) {
		t.Errorf("invalid mapping for tag 'go', bitmap: %s", merged.tags["go"])
	}

	tier1 := Pair{Key: "tier", Value: "1"}
	if !merged.annotations[tier1].Contains(2) || merged.annotations[tier1].Contains(0) {
		t.Errorf("invalid mapping for annotation tier=1, bitmap: %s", merged.annotations[tier1])
	}

	wantOldUnix := events1[0].StartTime.AsTime().Unix()
	gotOldUnix, ok := merged.startTime.GetValue(2)
	if !ok || gotOldUnix != wantOldUnix {
		t.Errorf("BSI value mismatch for rid=2: want %d, got %d (ok: %t)", wantOldUnix, gotOldUnix, ok)
	}

	wantNewUnix := events0[0].StartTime.AsTime().Unix()
	gotNewUnix, ok := merged.startTime.GetValue(0)
	if !ok || gotNewUnix != wantNewUnix {
		t.Errorf("BSI value mismatch for rid=0: want %d, got %d (ok: %t)", wantNewUnix, gotNewUnix, ok)
	}
}

func riFromEvents(t testing.TB, events []*storagepb.Event) *roaringIndex {
	t.Helper()
	ri := newRoaringIndex()
	for _, event := range events {
		indexEvent(ri, event)
	}
	return ri
}
