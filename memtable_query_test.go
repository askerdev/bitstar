package bitstar

import (
	"fmt"
	"testing"
	"time"

	storagepb "github.com/askerdev/bitstar/proto/infralenta/storage/v1"
	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestMemTableQuery_Do(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	uuidMax := "ffffffff-ffff-ffff-ffff-ffffffffffff"
	uuidMin := "00000000-0000-0000-0000-000000000000"

	events := [][]*storagepb.Event{
		{
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
		},
		{
			{
				Id:          uuid.Must(uuid.NewV7()).String(),
				StartTime:   timestamppb.New(now.Add(-2 * time.Hour)),
				EndTime:     timestamppb.New(now),
				Tags:        []string{"go", "backend"},
				Annotations: map[string]string{"env": "prod"},
			},
			{
				Id:          uuidMax,
				StartTime:   timestamppb.New(now.Add(-3 * time.Hour)),
				EndTime:     timestamppb.New(now),
				Tags:        []string{"go"},
				Annotations: map[string]string{"env": "dev"},
			},
		},
		{
			{
				Id:          uuidMin,
				StartTime:   timestamppb.New(now.Add(-3 * time.Hour)),
				EndTime:     timestamppb.New(now),
				Tags:        []string{"go", "backend"},
				Annotations: map[string]string{"env": "prod"},
			},
			{
				Id:          uuid.Must(uuid.NewV7()).String(),
				StartTime:   timestamppb.New(now.Add(-4 * time.Hour)),
				EndTime:     timestamppb.New(now),
				Tags:        []string{"go"},
				Annotations: map[string]string{"env": "dev"},
			},
		},
	}

	mts := make([]*memTable, 0, len(events))
	for _, events := range events {
		mt := newMemTable()
		for _, event := range events {
			mt.put(event, 0)
		}
		mts = append(mts, mt)
	}

	tc := []struct {
		q    *MemTableQuery
		want []*storagepb.Event
	}{
		{
			q: &MemTableQuery{
				StartTime: now,
				EndTime:   now,
				PageSize:  2,
				Filter:    parseFilter(t, `tags = "backend"`),
			},
			want: []*storagepb.Event{
				events[0][0],
				events[1][0],
				events[2][0],
			},
		},
		{
			q: &MemTableQuery{
				StartTime: now,
				EndTime:   now,
				PageSize:  2,
			},
			want: []*storagepb.Event{
				events[0][0],
				events[0][1],
				events[1][0],
				events[1][1],
				events[2][0],
				events[2][1],
			},
		},
	}

	for i, tt := range tc {
		t.Run(fmt.Sprintf("#%d", i), func(t *testing.T) {
			keys, hasNext, err := tt.q.Do(mts)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			for hasNext {
				tt.q.PageToken, _ = EncodePageToken(keys[len(keys)-1])
				nextKeys, localHasNext, err := tt.q.Do(mts)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				keys = append(keys, nextKeys...)
				hasNext = localHasNext
			}

			gotKeysPretty := make([]string, 0, len(keys))
			for _, key := range keys {
				gotKeysPretty = append(gotKeysPretty,
					fmt.Sprintf("%q/%q", key.StartTime.UTC(), key.ID),
				)
			}

			wantKeysPretty := make([]string, 0, len(tt.want))
			for i := range tt.want {
				wantKeysPretty = append(wantKeysPretty,
					fmt.Sprintf("%q/%q",
						tt.want[i].GetStartTime().AsTime(),
						tt.want[i].GetId(),
					),
				)
			}

			if diff := cmp.Diff(wantKeysPretty, gotKeysPretty); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
