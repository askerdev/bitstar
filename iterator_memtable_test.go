package bitstar

import (
	"fmt"
	"testing"
	"time"

	storagepb "github.com/askerdev/bitstar/proto/infralenta/storage/v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestIteratorMemTable(t *testing.T) {
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
		events    []item
		seek      *EventKey
		filter    string
		timestamp uint64
		want      []out
	}{
		{
			events: []item{
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
			},
			timestamp: 1,
			filter:    `tags = "go"`,
			want:      []out{{ids[0], 1}},
		},
		{
			events: []item{
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
			timestamp: 1,
			filter:    `tags = "go"`,
			want:      []out{{ids[0], 1}, {ids[1], 0}},
		},
		{
			events: []item{
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
			filter: `tags = "go"`,
			want:   []out{{ids[0], 0}, {ids[1], 0}},
		},
		{
			events: []item{
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
			filter: `tags = "backend"`,
			want:   []out{{ids[0], 0}},
		},
		{
			events: []item{
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
			events: []item{
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
			events: []item{
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
			mt := newMemTable()
			for _, item := range tt.events {
				mt.put(item.event, item.timestamp, false)
			}

			got := []out{}

			iter := NewMemTableIterator(mt, parseFilter(t, tt.filter), tt.timestamp)
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
