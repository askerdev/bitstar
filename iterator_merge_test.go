package bitstar

import (
	"fmt"
	"testing"
	"time"

	"github.com/askerdev/bitstar/filtering"
	storagepb "github.com/askerdev/bitstar/proto/infralenta/storage/v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestIteratorMerge(t *testing.T) {
	now := time.Now().Truncate(time.Second)

	ids := []string{
		uuid.Must(uuid.NewV7()).String(),
		uuid.Must(uuid.NewV7()).String(),
	}

	type item struct {
		event     *storagepb.Event
		timestamp uint64
	}

	type out struct {
		id        string
		timestamp uint64
	}

	tc := []struct {
		events    [][]item
		seek      *EventKey
		filter    string
		timestamp uint64
		want      []out
	}{
		{
			events: [][]item{
				{
					{
						event: &storagepb.Event{
							Id:          ids[0],
							StartTime:   timestamppb.New(now),
							EndTime:     timestamppb.New(now.Add(time.Hour)),
							Tags:        []string{"go", "backend"},
							Annotations: map[string]string{"env": "prod"},
						},
						timestamp: 2,
					},
				},
				{
					{
						event: &storagepb.Event{
							Id:          ids[1],
							StartTime:   timestamppb.New(now.Add(-time.Hour)),
							EndTime:     timestamppb.New(now),
							Tags:        []string{"go"},
							Annotations: map[string]string{"env": "dev"},
						},
					},
					{
						event: &storagepb.Event{
							Id:          ids[0],
							StartTime:   timestamppb.New(now),
							EndTime:     timestamppb.New(now.Add(time.Hour)),
							Tags:        []string{"go", "backend"},
							Annotations: map[string]string{"env": "prod"},
						},
						timestamp: 1,
					},
					{
						event: &storagepb.Event{
							Id:          ids[0],
							StartTime:   timestamppb.New(now),
							EndTime:     timestamppb.New(now.Add(time.Hour)),
							Tags:        []string{"go", "backend"},
							Annotations: map[string]string{"env": "prod"},
						},
					},
				},
			},
			timestamp: 2,
			filter:    `tags = "go"`,
			want:      []out{{ids[0], 2}, {ids[1], 0}},
		},
		{
			events: [][]item{
				{
					{
						event: &storagepb.Event{
							Id:          ids[1],
							StartTime:   timestamppb.New(now.Add(-time.Hour)),
							EndTime:     timestamppb.New(now),
							Tags:        []string{"go"},
							Annotations: map[string]string{"env": "dev"},
						},
					},
					{
						event: &storagepb.Event{
							Id:          ids[0],
							StartTime:   timestamppb.New(now),
							EndTime:     timestamppb.New(now.Add(time.Hour)),
							Tags:        []string{"go", "backend"},
							Annotations: map[string]string{"env": "prod"},
						},
						timestamp: 1,
					},
					{
						event: &storagepb.Event{
							Id:          ids[0],
							StartTime:   timestamppb.New(now),
							EndTime:     timestamppb.New(now.Add(time.Hour)),
							Tags:        []string{"go", "backend"},
							Annotations: map[string]string{"env": "prod"},
						},
					},
				},
			},
			filter: `tags = "go"`,
			want:   []out{{ids[0], 0}, {ids[1], 0}},
		},
		{
			events: [][]item{
				{
					{
						event: &storagepb.Event{
							Id:          ids[1],
							StartTime:   timestamppb.New(now.Add(-time.Hour)),
							EndTime:     timestamppb.New(now),
							Tags:        []string{"go"},
							Annotations: map[string]string{"env": "dev"},
						},
					},
					{
						event: &storagepb.Event{
							Id:          ids[0],
							StartTime:   timestamppb.New(now),
							EndTime:     timestamppb.New(now.Add(time.Hour)),
							Tags:        []string{"go", "backend"},
							Annotations: map[string]string{"env": "prod"},
						},
					},
				},
			},
			filter: `tags = "backend"`,
			want:   []out{{ids[0], 0}},
		},
		{
			events: [][]item{
				{
					{
						event: &storagepb.Event{
							Id:          ids[1],
							StartTime:   timestamppb.New(now.Add(-time.Hour)),
							EndTime:     timestamppb.New(now),
							Tags:        []string{"go"},
							Annotations: map[string]string{"env": "dev"},
						},
					},
					{
						event: &storagepb.Event{
							Id:          ids[0],
							StartTime:   timestamppb.New(now),
							EndTime:     timestamppb.New(now.Add(time.Hour)),
							Tags:        []string{"go", "backend"},
							Annotations: map[string]string{"env": "prod"},
						},
					},
				},
			},
			seek: &EventKey{
				ResourceTypeCode:   "abc_application",
				ResourceExternalID: ids[0],
				StartTime:          now,
				ID:                 ids[0],
			},
			filter: `tags = "go"`,
			want:   []out{{ids[0], 0}, {ids[1], 0}},
		},
		{
			events: [][]item{
				{
					{
						event: &storagepb.Event{
							Id:          ids[1],
							StartTime:   timestamppb.New(now.Add(-time.Hour)),
							EndTime:     timestamppb.New(now),
							Tags:        []string{"go"},
							Annotations: map[string]string{"env": "dev"},
						},
					},
					{
						event: &storagepb.Event{
							Id:          ids[0],
							StartTime:   timestamppb.New(now),
							EndTime:     timestamppb.New(now.Add(time.Hour)),
							Tags:        []string{"go", "backend"},
							Annotations: map[string]string{"env": "prod"},
						},
					},
				},
			},
			seek: &EventKey{
				ResourceTypeCode:   "abc_application",
				ResourceExternalID: ids[1],
				StartTime:          now.Add(-time.Minute),
				ID:                 ids[1],
			},
			filter: `tags = "go"`,
			want:   []out{{ids[1], 0}},
		},
		{
			events: [][]item{
				{
					{
						event: &storagepb.Event{
							Id:          ids[1],
							StartTime:   timestamppb.New(now.Add(-time.Hour)),
							EndTime:     timestamppb.New(now),
							Tags:        []string{"go"},
							Annotations: map[string]string{"env": "dev"},
						},
					},
					{
						event: &storagepb.Event{
							Id:          ids[0],
							StartTime:   timestamppb.New(now),
							EndTime:     timestamppb.New(now.Add(time.Hour)),
							Tags:        []string{"go", "backend"},
							Annotations: map[string]string{"env": "prod"},
						},
					},
				},
			},
			seek: &EventKey{
				ResourceTypeCode:   "abc_application",
				ResourceExternalID: ids[1],
				StartTime:          now.Add(-2 * time.Hour),
				ID:                 ids[1],
			},
			filter: `tags = "go"`,
			want:   []out{},
		},
	}

	for i, tt := range tc {
		t.Run(fmt.Sprintf("#%d", i), func(t *testing.T) {
			filter := parseFilter(t, tt.filter)

			iterators := make([]Iterator, 0, len(tt.events))
			for _, events := range tt.events {
				mt := newMemTable()
				for _, item := range events {
					mt.put(item.event, item.timestamp, false)
				}
				iterators = append(iterators, NewMemTableIterator(mt, filter, tt.timestamp))
			}

			got := []out{}

			iter := NewMergeIterator(iterators)
			if tt.seek != nil {
				iter.Seek(*tt.seek)
			}

			for iter.Next() {
				got = append(got, out{iter.Value().ID, iter.Value().Timestamp})
			}

			assert.Equal(t, tt.want, got)
		})
	}
}

func parseFilter(t testing.TB, filter string) *filtering.Filter {
	t.Helper()
	f, err := filtering.ParseFilter(filter)
	if err != nil {
		t.Fatalf("parse filter fail: %v", err)
	}
	return f
}
