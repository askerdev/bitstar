package bitstar

import (
	"fmt"
	"testing"
	"time"

	storagepb "github.com/askerdev/bitstar/proto/infralenta/storage/v1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestIteratorIndex(t *testing.T) {
	now := time.Now().Truncate(time.Second)

	ids := []string{
		uuid.Must(uuid.NewV7()).String(),
		uuid.Must(uuid.NewV7()).String(),
	}

	tc := []struct {
		events []*storagepb.Event
		seek   *EventKey
		filter string
		want   []string
	}{
		{
			events: []*storagepb.Event{
				{
					Id:          ids[0],
					StartTime:   timestamppb.New(now),
					EndTime:     timestamppb.New(now.Add(time.Hour)),
					Tags:        []string{"go", "backend"},
					Annotations: map[string]string{"env": "prod"},
				},
				{
					Id:          ids[1],
					StartTime:   timestamppb.New(now.Add(-time.Hour)),
					EndTime:     timestamppb.New(now),
					Tags:        []string{"go"},
					Annotations: map[string]string{"env": "dev"},
				},
			},
			filter: `tags = "go"`,
			want:   []string{ids[0], ids[1]},
		},
		{
			events: []*storagepb.Event{
				{
					Id:          ids[0],
					StartTime:   timestamppb.New(now),
					EndTime:     timestamppb.New(now.Add(time.Hour)),
					Tags:        []string{"go", "backend"},
					Annotations: map[string]string{"env": "prod"},
				},
				{
					Id:          ids[1],
					StartTime:   timestamppb.New(now.Add(-time.Hour)),
					EndTime:     timestamppb.New(now),
					Tags:        []string{"go"},
					Annotations: map[string]string{"env": "dev"},
				},
			},
			filter: `tags = "backend"`,
			want:   []string{ids[0]},
		},
		{
			events: []*storagepb.Event{
				{
					Id: ids[0],
					Resource: &storagepb.Resource{
						TypeCode:   "abc_application",
						ExternalId: ids[0],
					},
					StartTime:   timestamppb.New(now),
					EndTime:     timestamppb.New(now.Add(time.Hour)),
					Tags:        []string{"go", "backend"},
					Annotations: map[string]string{"env": "prod"},
				},
				{
					Id: ids[1],
					Resource: &storagepb.Resource{
						TypeCode:   "abc_application",
						ExternalId: ids[1],
					},
					StartTime:   timestamppb.New(now.Add(-time.Hour)),
					EndTime:     timestamppb.New(now),
					Tags:        []string{"go"},
					Annotations: map[string]string{"env": "dev"},
				},
			},
			seek: &EventKey{
				ResourceTypeCode:   "abc_application",
				ResourceExternalID: ids[0],
				StartTime:          now,
				ID:                 ids[0],
			},
			filter: `tags = "go"`,
			want:   []string{ids[0], ids[1]},
		},
		{
			events: []*storagepb.Event{
				{
					Id: ids[0],
					Resource: &storagepb.Resource{
						TypeCode:   "abc_application",
						ExternalId: ids[0],
					},
					StartTime:   timestamppb.New(now),
					EndTime:     timestamppb.New(now.Add(time.Hour)),
					Tags:        []string{"go", "backend"},
					Annotations: map[string]string{"env": "prod"},
				},
				{
					Id: ids[1],
					Resource: &storagepb.Resource{
						TypeCode:   "abc_application",
						ExternalId: ids[1],
					},
					StartTime:   timestamppb.New(now.Add(-time.Hour)),
					EndTime:     timestamppb.New(now),
					Tags:        []string{"go"},
					Annotations: map[string]string{"env": "dev"},
				},
			},
			seek: &EventKey{
				ResourceTypeCode:   "abc_application",
				ResourceExternalID: ids[1],
				StartTime:          now.Add(-time.Minute),
				ID:                 ids[1],
			},
			filter: `tags = "go"`,
			want:   []string{ids[1]},
		},
		{
			events: []*storagepb.Event{
				{
					Id: ids[0],
					Resource: &storagepb.Resource{
						TypeCode:   "abc_application",
						ExternalId: ids[0],
					},
					StartTime:   timestamppb.New(now),
					EndTime:     timestamppb.New(now.Add(time.Hour)),
					Tags:        []string{"go", "backend"},
					Annotations: map[string]string{"env": "prod"},
				},
				{
					Id: ids[1],
					Resource: &storagepb.Resource{
						TypeCode:   "abc_application",
						ExternalId: ids[1],
					},
					StartTime:   timestamppb.New(now.Add(-time.Hour)),
					EndTime:     timestamppb.New(now),
					Tags:        []string{"go"},
					Annotations: map[string]string{"env": "dev"},
				},
			},
			seek: &EventKey{
				ResourceTypeCode:   "abc_application",
				ResourceExternalID: ids[1],
				StartTime:          now.Add(-2 * time.Hour),
				ID:                 ids[1],
			},
			filter: `tags = "go"`,
			want:   []string{},
		},
	}

	for i, tt := range tc {
		t.Run(fmt.Sprintf("#%d", i), func(t *testing.T) {
			ri := riFromEvents(t, tt.events)

			posting, err := ri.query(now, now, parseFilter(t, tt.filter))
			require.NoError(t, err)

			got := []string{}

			iter := NewIndexIterator(ri, posting, 0)
			if tt.seek != nil {
				iter.Seek(*tt.seek)
			}

			for iter.Next() {
				got = append(got, iter.Value().ID)
			}

			assert.Equal(t, tt.want, got)
		})
	}
}
