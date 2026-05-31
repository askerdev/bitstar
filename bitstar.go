package bitstar

import (
	"time"

	"github.com/google/uuid"
)

type Event struct {
	ID          uuid.UUID         `json:"id"`
	Title       string            `json:"title"`
	Description string            `json:"description"`
	StartTime   time.Time         `json:"start_time"`
	EndTime     time.Time         `json:"end_time"`
	Tags        []string          `json:"tags"`
	Annotations map[string]string `json:"annotations"`
}

type Filter struct {
	And []Or `json:"and"`
}

type Or struct {
	Or []EventFilter `json:"or"`
}

type EventFilter struct {
	Tag        string `json:"tag"`
	Annotation Pair   `json:"annotation"`
}

type Pair struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}
