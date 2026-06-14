package bitstar

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	storagepb "github.com/askerdev/bitstar/proto/infralenta/storage/v1"
	"github.com/google/uuid"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	eventsCols = []string{
		"resource_type_code",
		"resource_external_id",
		"start_time",
		"id",
		"title",
		"description",
		"end_time",
		"tags",
		"annotations",
		"created_by",
		"updated_by",
		"create_time",
		"update_time",
		"delete_time",
	}
)

func forEachEvent(ctx context.Context, result result.BaseResult, f func(*storagepb.Event) error) error {
	defer result.Close()

	for result.NextResultSet(ctx, eventsCols...) {
		for result.NextRow() {
			event := &storagepb.Event{Resource: &storagepb.Resource{}}

			if err := scanEvent(result, event); err != nil {
				return err
			}

			if err := f(event); err != nil {
				return err
			}
		}
	}

	return result.Err()
}

func scanEvent(result result.BaseResult, event *storagepb.Event) error {
	if event.Resource == nil {
		event.Resource = &storagepb.Resource{}
	}

	var (
		startTime       time.Time
		idVal           uuid.UUID
		endTime         time.Time
		createdBy       string
		updatedBy       string
		createTime      time.Time
		updateTime      *time.Time
		deleteTime      *time.Time
		tagsRaw, annRaw []byte
	)

	err := result.ScanNamed(
		named.Required("resource_type_code", &event.Resource.TypeCode),
		named.Required("resource_external_id", &event.Resource.ExternalId),
		named.Required("start_time", &startTime),
		named.Required("id", &idVal),
		named.Required("title", &event.Title),
		named.Required("description", &event.Description),
		named.Required("end_time", &endTime),
		named.Required("tags", &tagsRaw),
		named.Required("annotations", &annRaw),
		named.Required("created_by", &createdBy),
		named.Required("updated_by", &updatedBy),
		named.Required("create_time", &createTime),
		named.Optional("update_time", &updateTime),
		named.Optional("delete_time", &deleteTime),
	)
	if err != nil {
		return fmt.Errorf("stream scan named failed: %w", err)
	}

	if len(tagsRaw) > 0 && !bytes.Equal(tagsRaw, []byte("[]")) {
		if err := json.Unmarshal(tagsRaw, &event.Tags); err != nil {
			return err
		}
	}

	if len(annRaw) > 0 && !bytes.Equal(annRaw, []byte("{}")) {
		if err := json.Unmarshal(annRaw, &event.Annotations); err != nil {
			return err
		}
	}

	event.Id = idVal.String()
	event.StartTime = timestamppb.New(startTime)
	event.EndTime = timestamppb.New(endTime)
	event.CreatedBy = createdBy
	event.UpdatedBy = updatedBy
	event.CreateTime = timestamppb.New(createTime)

	if updateTime != nil {
		event.UpdateTime = timestamppb.New(*updateTime)
	}

	if deleteTime != nil {
		event.DeleteTime = timestamppb.New(*deleteTime)
	}

	return nil
}
