package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"slices"
	"strconv"
	"time"

	storagepb "github.com/askerdev/bitstar/proto/infralenta/storage/v1"
	"github.com/google/uuid"
	"go.ytsaurus.tech/yt/go/yson"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
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

	const maxMsgSize = 134217728

	conn, err := grpc.NewClient(
		"localhost:11080",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxMsgSize),
			grpc.MaxCallSendMsgSize(maxMsgSize),
		),
	)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	client := storagepb.NewEventServiceClient(conn)

	rowCount := int64(0)
	errorCount := int64(0)
	startTime := time.Now()

	fmt.Printf("Starting to process file: %s\n", filename)

	reader := yson.NewReaderKind(file, yson.StreamListFragment)

	batch := []*storagepb.CreateEventRequest{}
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

		params := &storagepb.CreateEventRequest{
			Event: &storagepb.Event{
				Title:       strptr(event.Title),
				Description: strptr(event.Description),
				StartTime:   timestamppb.New(time.Unix(int64(*event.StartTime), 0)),
				Annotations: map[string]string{
					"resource_type_code":   resourceTypeCode(event.ResourceManagerEntity.Type),
					"resource_external_id": event.ResourceManagerEntity.Slug,
					"created_by":           strptr(event.CreatedBy),
					"updated_by":           strptr(event.UpdatedBy),
					"service_id":           int32strptr(event.ServiceID),
					"environment_id":       int32strptr(event.EnvironmentID),
					"type":                 strptr(event.Type),
					"severity":             strptr(event.Severity),
					"tickets":              strptr(event.Tickets),
					"meta":                 strptr(event.Meta),
					"calendar_event_id":    int32strptr(event.CalendarEventID),
					"is_deleted":           boolstrptr(event.IsDeleted),
					"attendees":            string(convertStringSliceToJSON(event.Attendees)),
					"infralenta_id":        strptr(event.InfralentaID),
					"created_at":           microsecondsToTime(event.CreatedAt).String(),
					"updated_at":           microsecondsToTime(event.UpdatedAt).String(),
				},
			},
		}

		endTime := NullEndTime
		if event.FinishTime != nil {
			endTime = time.Unix(int64(*event.FinishTime), 0)
		}
		params.Event.EndTime = timestamppb.New(endTime)

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
			err := writeBatch(client, context.Background(), batch)
			if err != nil {
				log.Printf("batch insert error: %v", err)
				errorCount += int64(len(batch))
			} else {
				fmt.Printf("Inserted batch of %d rows. Total: %d\n", len(batch), rowCount)
			}
			batch = []*storagepb.CreateEventRequest{}
		}

		if rowCount <= 10 {
			fmt.Printf("Row %d: Event ID: %v, Service ID: %v, Title: %.50s...\n",
				rowCount, event.ID, event.ServiceID, strptr(event.Title))
		}
	}

	if len(batch) > 0 {
		err := writeBatch(client, context.Background(), batch)
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

func writeBatch(client storagepb.EventServiceClient, _ context.Context, in []*storagepb.CreateEventRequest) error {
	eg := &errgroup.Group{}
	chunks := slices.Chunk(in, 512)
	for chunk := range chunks {
		eg.Go(func() error {
			start := time.Now()
			_, err := client.BatchCreateEvents(context.Background(), &storagepb.BatchCreateEventsRequest{Requests: chunk})
			if err == nil {
				fmt.Println("wrote batch, elapsed", time.Since(start).String())
			}
			return err
		})
	}
	return eg.Wait()
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
