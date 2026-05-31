package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/google/uuid"
	"go.ytsaurus.tech/yt/go/yson"
)

type ResourceManagerEntity struct {
	Type string `yson:"type"`
	Slug string `yson:"slug"`
}

type Event struct {
	Attendees             []string              `yson:"events.attendees"`
	CalendarEventID       *int32                `yson:"events.calendar_event_id"`
	Components            []string              `yson:"events.components"`
	CreatedAt             *uint64               `yson:"events.created_at"`
	CreatedBy             *string               `yson:"events.created_by"`
	Dcs                   []string              `yson:"events.dcs"`
	Description           *string               `yson:"events.description"`
	EnvironmentID         *int32                `yson:"events.environment_id"`
	FinishTime            *int32                `yson:"events.finish_time"`
	ID                    *int32                `yson:"events.id"`
	InfralentaID          *string               `yson:"events.infralenta_id"`
	IsDeleted             *bool                 `yson:"events.is_deleted"`
	Meta                  *string               `yson:"events.meta"`
	ServiceID             *int32                `yson:"events.service_id"`
	Severity              *string               `yson:"events.severity"`
	StartTime             *int32                `yson:"events.start_time"`
	Tickets               *string               `yson:"events.tickets"`
	Title                 *string               `yson:"events.title"`
	Type                  *string               `yson:"events.type"`
	UpdatedAt             *uint64               `yson:"events.updated_at"`
	UpdatedBy             *string               `yson:"events.updated_by"`
	ResourceManagerEntity ResourceManagerEntity `yson:"services.resource_manager_entity"`
}

var NullEndTime = time.Unix(int64(uint64(math.MaxUint32)), 0)

type Resource struct {
	TypeCode   string `json:"type_code"`
	ExternalID string `json:"external_id"`
}

type BitstarEvent struct {
	ID          uuid.UUID         `json:"id"`
	Title       string            `json:"title"`
	Description string            `json:"description"`
	Resource    Resource          `json:"resource"`
	StartTime   time.Time         `json:"start_time"`
	EndTime     time.Time         `json:"end_time"`
	Tags        []string          `json:"tags"`
	Annotations map[string]string `json:"annotations"`
}

type BatchCreateEventRequest struct {
	Requests []CreateEventRequest `json:"requests"`
}

type CreateEventRequest struct {
	Event *BitstarEvent `json:"event"`
}

func main() {
	pathFlag := flag.String("path", "", "path to yson list of events")
	flag.Parse()
	if pathFlag == nil || len(*pathFlag) == 0 {
		panic("path flag is required")
	}

	filename := *pathFlag

	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	rowCount := int64(0)
	errorCount := int64(0)
	startTime := time.Now()

	fmt.Printf("Starting to process file: %s\n", filename)

	reader := yson.NewReaderKind(file, yson.StreamListFragment)

	batch := []CreateEventRequest{}
	const batchSize = 128 * 1024

	saw := make(map[int32]struct{})

	for {
		ok, err := reader.NextListItem()
		if err != nil {
			log.Printf("reader error: %v", err)
			return
		}
		if !ok {
			break
		}

		var event Event
		d := yson.Decoder{R: reader}
		if err := d.Decode(&event); err != nil {
			log.Printf("decode row: %v", err)
			return
		}

		if event.ServiceID == nil {
			continue
		}

		if _, ok := saw[*event.ID]; ok {
			continue
		}

		saw[*event.ID] = struct{}{}

		params := CreateEventRequest{
			Event: &BitstarEvent{
				Title:       strptr(event.Title),
				Description: strptr(event.Description),
				Resource: Resource{
					TypeCode:   resourceTypeCode(event.ResourceManagerEntity.Type),
					ExternalID: event.ResourceManagerEntity.Slug,
				},
				StartTime: time.Unix(int64(*event.StartTime), 0),
				Annotations: map[string]string{
					"created_by":        strptr(event.CreatedBy),
					"updated_by":        strptr(event.UpdatedBy),
					"service_id":        int32strptr(event.ServiceID),
					"environment_id":    int32strptr(event.EnvironmentID),
					"type":              strptr(event.Type),
					"severity":          strptr(event.Severity),
					"tickets":           strptr(event.Tickets),
					"meta":              strptr(event.Meta),
					"calendar_event_id": int32strptr(event.CalendarEventID),
					"is_deleted":        boolstrptr(event.IsDeleted),
					"attendees":         string(convertStringSliceToJSON(event.Attendees)),
					"infralenta_id":     strptr(event.InfralentaID),
					"created_at":        microsecondsToTime(event.CreatedAt).String(),
					"updated_at":        microsecondsToTime(event.UpdatedAt).String(),
				},
			},
		}

		endTime := NullEndTime
		if event.FinishTime != nil {
			endTime = time.Unix(int64(*event.FinishTime), 0)
		}
		params.Event.EndTime = endTime

		if len(event.Components) > 0 {
			params.Event.Tags = append(params.Event.Tags, event.Components...)
		}

		if len(event.Dcs) > 0 {
			for _, dc := range event.Dcs {
				params.Event.Tags = append(params.Event.Tags, "dc:"+dc)
			}
		}

		batch = append(batch, params)
		rowCount++

		if len(batch) >= batchSize {
			err := batchCreateEvents(context.Background(), &BatchCreateEventRequest{Requests: batch})
			if err != nil {
				log.Printf("batch insert error: %v", err)
				errorCount += int64(len(batch))
			} else {
				fmt.Printf("Inserted batch of %d rows. Total: %d\n", len(batch), rowCount)
			}
			batch = []CreateEventRequest{}
		}

		if rowCount <= 10 {
			fmt.Printf("Row %d: Event ID: %v, Service ID: %v, Title: %.50s...\n",
				rowCount, event.ID, event.ServiceID, strptr(event.Title))
		}
	}

	if len(batch) > 0 {
		err := batchCreateEvents(context.Background(), &BatchCreateEventRequest{Requests: batch})
		if err != nil {
			log.Printf("final batch insert error: %v", err)
			errorCount += int64(len(batch))
		} else {
			fmt.Printf("Inserted final batch of %d rows. Total: %d\n", len(batch), rowCount)
		}
	}

	elapsed := time.Since(startTime)
	fmt.Printf("\n=== Processing Complete ===\n")
	fmt.Printf("Total rows processed: %d\n", rowCount)
	fmt.Printf("Errors encountered: %d\n", errorCount)
	fmt.Printf("Total time: %v\n", elapsed)
	fmt.Printf("Average rate: %.0f rows/sec\n", float64(rowCount)/elapsed.Seconds())
}

func batchCreateEvents(ctx context.Context, in *BatchCreateEventRequest) error {
	body, _ := json.Marshal(in)

	request, err := http.NewRequestWithContext(
		ctx,
		"POST",
		"http://localhost:8080/v1/events:batchCreate",
		bytes.NewReader(body),
	)
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		bytes, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}
		return fmt.Errorf("request failed, body %q", string(bytes))
	}

	return nil
}

func resourceTypeCode(resourceType string) string {
	switch resourceType {
	case "application":
		return "abc_application"
	case "product":
		return "idp_product"
	case "team":
		return "team"
	default:
		panic(fmt.Sprintf("unknown resource manager entity type: %s", resourceType))
	}
}

func strptr(s *string) string {
	if s == nil {
		return "<nil>"
	}
	return *s
}

func int32strptr(i *int32) string {
	if i == nil {
		return "<nil>"
	}
	return strconv.Itoa(int(*i))
}

func boolstrptr(b *bool) string {
	if b == nil {
		return "<nil>"
	}
	return strconv.FormatBool(*b)
}

func convertStringSliceToJSON(slice []string) []byte {
	if slice == nil {
		return nil
	}
	jsonBytes, err := json.Marshal(slice)
	if err != nil {
		return nil
	}
	return jsonBytes
}

func microsecondsToTime(timestamp *uint64) time.Time {
	if timestamp == nil || *timestamp == 0 {
		return NullEndTime
	}

	seconds := int64(*timestamp / 1000000)
	nanoseconds := int64(*timestamp%1000000) * 1000

	return time.Unix(seconds, nanoseconds)
}
