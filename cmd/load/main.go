package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	storagepb "github.com/askerdev/bitstar/proto/infralenta/storage/v1"
	"go.ytsaurus.tech/yt/go/yson"
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

var NullEndTime = time.Date(2105, time.December, 31, 23, 59, 59, 0, time.UTC)

func main() {
	pathFlag := flag.String("path", "", "path to yson list of events")
	workersFlag := flag.Int("workers", 20, "number of concurrent gRPC clients")
	batchSizeFlag := flag.Int("batch", 500, "number of events per gRPC request")
	timeoutFlag := flag.Duration("timeout", 5*time.Second, "gRPC request timeout")
	flag.Parse()

	if *pathFlag == "" {
		log.Fatal("path flag is required")
	}

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
		log.Fatal(err)
	}
	defer conn.Close()

	client := storagepb.NewEventServiceClient(conn)

	// Каналы для пайплайна
	jobs := make(chan *storagepb.CreateEventRequest, (*workersFlag)*(*batchSizeFlag)*2)
	results := make(chan time.Duration, 2000000)

	var wg sync.WaitGroup
	var totalSent atomic.Int64
	var totalErrors atomic.Int64

	fmt.Printf("Starting load test: %d workers, batch size %d\n", *workersFlag, *batchSizeFlag)
	startTime := time.Now()

	// 1. Запускаем Worker Pool (Конкурентные клиенты)
	for i := 0; i < *workersFlag; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			batch := make([]*storagepb.CreateEventRequest, 0, *batchSizeFlag)

			sendBatch := func() {
				if len(batch) == 0 {
					return
				}

				ctx, cancel := context.WithTimeout(context.Background(), *timeoutFlag)
				defer cancel()

				reqStart := time.Now()
				_, err := client.BatchCreateEvents(ctx, &storagepb.BatchCreateEventsRequest{Requests: batch})
				duration := time.Since(reqStart)

				if err != nil {
					totalErrors.Add(int64(len(batch)))
					// log.Printf("batch error: %v", err) // раскомментируй для дебага
				} else {
					totalSent.Add(int64(len(batch)))
					results <- duration
				}

				batch = batch[:0] // сброс батча с сохранением capacity
			}

			for job := range jobs {
				batch = append(batch, job)
				if len(batch) >= *batchSizeFlag {
					sendBatch()
				}
			}
			sendBatch() // Досылаем остатки
		}()
	}

	// 2. Фоновый мониторинг RPS
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		var lastSent int64

		for range ticker.C {
			currentSent := totalSent.Load()
			delta := currentSent - lastSent
			lastSent = currentSent
			fmt.Printf("[Monitor] Sent: %d, Errors: %d, Current RPS: %d\n",
				currentSent, totalErrors.Load(), delta/2)
		}
	}()

	// 3. Чтение файла (Producer)
	file, err := os.Open(*pathFlag)
	if err != nil {
		log.Fatal(err)
	}

	reader := yson.NewReaderKind(file, yson.StreamListFragment)
	saw := make(map[int32]struct{})
	rowCount := 0

	for {
		ok, err := reader.NextListItem()
		if err != nil {
			log.Printf("reader error: %v", err)
			break
		}
		if !ok {
			break
		}

		var event Event
		d := yson.Decoder{R: reader}
		if err := d.Decode(&event); err != nil {
			log.Printf("decode row: %v", err)
			break
		}

		if event.ServiceID == nil {
			continue
		}
		if _, exists := saw[*event.ID]; exists {
			continue
		}
		saw[*event.ID] = struct{}{}

		params := mapEventToProto(&event) // Вынесли маппинг в отдельную функцию для чистоты
		jobs <- params
		rowCount++
	}

	file.Close()
	close(jobs) // Сигнал воркерам, что данных больше нет

	// 4. Ожидание завершения
	wg.Wait()
	close(results)

	// 5. Подсчет статистики и перцентилей
	var latencies []time.Duration
	var totalDuration time.Duration
	for r := range results {
		latencies = append(latencies, r)
		totalDuration += r
	}

	elapsed := time.Since(startTime)

	fmt.Printf("\n=== Load Test Complete ===\n")
	fmt.Printf("Total events processed : %d\n", rowCount)
	fmt.Printf("Total events sent      : %d\n", totalSent.Load())
	fmt.Printf("Total errors           : %d\n", totalErrors.Load())
	fmt.Printf("Total test time        : %v\n", elapsed)
	fmt.Printf("Average throughput     : %.0f events/sec\n", float64(totalSent.Load())/elapsed.Seconds())

	if len(latencies) > 0 {
		slices.Sort(latencies)
		fmt.Printf("\n=== Batch Latency Metrics (Batch Size: %d) ===\n", *batchSizeFlag)
		fmt.Printf("Total requests : %d\n", len(latencies))
		fmt.Printf("Min            : %v\n", latencies[0])
		fmt.Printf("Mean           : %v\n", totalDuration/time.Duration(len(latencies)))
		fmt.Printf("p50 (Median)   : %v\n", latencies[len(latencies)*50/100])
		fmt.Printf("p95            : %v\n", latencies[len(latencies)*95/100])
		fmt.Printf("p99            : %v\n", latencies[len(latencies)*99/100])
		fmt.Printf("Max            : %v\n", latencies[len(latencies)-1])
	}
}

// Вынесенная логика маппинга
func mapEventToProto(event *Event) *storagepb.CreateEventRequest {
	req := &storagepb.CreateEventRequest{
		Event: &storagepb.Event{
			Title:       strptr(event.Title),
			Description: strptr(event.Description),
			Resource: &storagepb.Resource{
				TypeCode:   resourceTypeCode(event.ResourceManagerEntity.Type),
				ExternalId: event.ResourceManagerEntity.Slug,
			},
			StartTime: timestamppb.New(time.Unix(int64(*event.StartTime), 0)),
			Annotations: map[string]string{
				"resource_type_code":   resourceTypeCode(event.ResourceManagerEntity.Type),
				"resource_external_id": event.ResourceManagerEntity.Slug,
				"service_id":           int32strptr(event.ServiceID),
				// ... (остальные поля из твоего кода) ...
				"created_at": microsecondsToTime(event.CreatedAt).String(),
			},
		},
	}

	endTime := NullEndTime
	if event.FinishTime != nil {
		endTime = time.Unix(int64(*event.FinishTime), 0)
	}
	req.Event.EndTime = timestamppb.New(endTime)

	if len(event.Components) > 0 {
		req.Event.Tags = append(req.Event.Tags, event.Components...)
	}
	if len(event.Dcs) > 0 {
		for _, dc := range event.Dcs {
			req.Event.Tags = append(req.Event.Tags, "dc:"+dc)
		}
	}
	return req
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
