package bitstar

import (
	"testing"

	"github.com/askerdev/bitstar/filtering"
	storagepb "github.com/askerdev/bitstar/proto/infralenta/storage/v1"
	"github.com/bits-and-blooms/bloom/v3"
)

func TestMatchMemTableItem(t *testing.T) {
	event := &storagepb.Event{
		Id:    "event-1",
		Title: "Deploy Service",
		Tags:  []string{"prod", "backend", "critical"},
		Annotations: map[string]string{
			"env":    "production",
			"team":   "infra",
			"region": "eu-west",
		},
	}

	item := &memTableItem{
		event:       event,
		tags:        bloom.NewWithEstimates(50, 0.01),
		annotations: bloom.NewWithEstimates(50, 0.01),
	}

	for _, tag := range event.Tags {
		item.tags.AddString(tag)
	}
	for k, v := range event.Annotations {
		item.annotations.AddString(k + "_" + v)
	}

	tests := []struct {
		name    string
		filter  string
		want    bool
		wantErr bool
	}{
		{
			name:   "Empty filter matches everything",
			filter: "",
			want:   true,
		},
		{
			name:   "Simple tags match",
			filter: `tags = "prod"`,
			want:   true,
		},
		{
			name:   "Simple tags mismatch",
			filter: `tags = "frontend"`,
			want:   false,
		},
		{
			name:   "Simple annotations match",
			filter: `annotations.env = "production"`,
			want:   true,
		},
		{
			name:   "Simple annotations mismatch",
			filter: `annotations.env = "staging"`,
			want:   false,
		},
		{
			name:   "Annotation key does not exist",
			filter: `annotations.missing_key = "value"`,
			want:   false,
		},
		{
			name:   "AND operator - both true",
			filter: `tags = "prod" AND annotations.env = "production"`,
			want:   true,
		},
		{
			name:   "AND operator - one false",
			filter: `tags = "prod" AND annotations.env = "staging"`,
			want:   false,
		},
		{
			name:   "OR operator - one true",
			filter: `tags = "frontend" OR annotations.team = "infra"`,
			want:   true,
		},
		{
			name:   "OR operator - both false",
			filter: `tags = "frontend" OR annotations.team = "frontend"`,
			want:   false,
		},
		{
			name:   "NOT operator - negating false condition",
			filter: `NOT tags = "frontend"`,
			want:   true,
		},
		{
			name:   "NOT operator - negating true condition",
			filter: `NOT annotations.region = "eu-west"`,
			want:   false,
		},
		{
			name:   "Composite complex expression",
			filter: `(tags = "prod" AND annotations.team = "infra") AND NOT annotations.region = "us-east"`,
			want:   true,
		},
		{
			name:   "Implicit AND (Sequence of Factors without explicit AND)",
			filter: `tags = "prod" annotations.env = "production"`,
			want:   true,
		},
		{
			name:    "Error - tags type mismatch (not quoted)",
			filter:  `tags = unquoted_val`,
			want:    false,
			wantErr: true,
		},
		{
			name:    "Error - annotations type mismatch (not quoted)",
			filter:  `annotations.env = unquoted_val`,
			want:    false,
			wantErr: true,
		},
		{
			name:    "Error - unknown field filtering",
			filter:  `title = "Deploy Service"`,
			want:    false,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var parsedFilter *filtering.Filter
			var err error

			if tt.filter != "" {
				parsedFilter, err = filtering.ParseFilter(tt.filter)
				if err != nil {
					t.Fatalf("failed to parse filter %q: %v", tt.filter, err)
				}
			}

			got, err := matchMemTableItem(parsedFilter, item)
			if (err != nil) != tt.wantErr {
				t.Errorf("MatchEventWithBloom() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("MatchEventWithBloom() got = %v, want %v for filter: %s", got, tt.want, tt.filter)
			}
		})
	}
}
