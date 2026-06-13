package bitstar

import (
	"encoding/base64"
	"fmt"
	"testing"
	"time"

	storagepb "github.com/askerdev/bitstar/proto/infralenta/storage/v1"
	"github.com/dgraph-io/badger/v4/skl"
	"github.com/dgraph-io/badger/v4/y"
	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestScanQuery_Do(t *testing.T) {
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

	skls := make([]*skl.Skiplist, 0, len(events))
	for _, events := range events {
		l := skl.NewSkiplist(256 << 20)
		for _, event := range events {
			key := encodeKey(event.GetStartTime().AsTime(), uuid.MustParse(event.GetId()))

			val, err := proto.Marshal(event)
			if err != nil {
				t.Fatalf("proto marshal fail: %v", err)
			}

			l.Put(key, y.ValueStruct{
				Value:    val,
				Meta:     0,
				UserMeta: 0,
			})
		}
		skls = append(skls, l)
	}

	tc := []struct {
		q    *ScanQuery
		want []*storagepb.Event
	}{
		{
			q: &ScanQuery{
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
			q: &ScanQuery{
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
			keys, hasNext, err := tt.q.Do(skls)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			next := hasNext
			for next {
				tt.q.PageToken = base64.StdEncoding.EncodeToString(keys[len(keys)-1])
				nextKeys, hasNext, err := tt.q.Do(skls)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				keys = append(keys, nextKeys...)
				next = hasNext
			}

			gotKeysPretty := make([]string, 0, len(keys))
			for _, key := range keys {
				startTime, id := decodeKey(key)
				gotKeysPretty = append(gotKeysPretty,
					fmt.Sprintf("%q/%q", startTime.UTC(), id),
				)
			}

			wantKeysPretty := make([]string, 0, len(tt.want))
			for i := range tt.want {
				wantKeysPretty = append(wantKeysPretty,
					fmt.Sprintf("%q/%q",
						tt.want[i].GetStartTime().AsTime(),
						uuid.MustParse(tt.want[i].GetId()),
					),
				)
			}

			if diff := cmp.Diff(wantKeysPretty, gotKeysPretty); diff != "" {
				t.Errorf("mismatch (-want +got):\n%s", diff)
			}
		})
	}
}
