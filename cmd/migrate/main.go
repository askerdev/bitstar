package main

import (
	"context"

	"go.ytsaurus.tech/library/go/ptr"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ythttp"
	"go.ytsaurus.tech/yt/go/yterrors"
)

const (
	initialTabletCount = 128
)

func main() {
	ctx := context.Background()

	yc, err := ythttp.NewClient(&yt.Config{Proxy: "localhost:8000"})
	if err != nil {
		panic(err)
	}

	_, err = yt.CreateTable(
		ctx,
		yc,
		ypath.Path("//home/events"),
		yt.WithAttributes(map[string]any{
			"dynamic":      true,
			"optimize_for": "lookup",
		}),
		yt.WithSchema(schema.Schema{
			UniqueKeys: true,
			Columns: []schema.Column{
				{
					Name:       "hash",
					Type:       schema.TypeUint64,
					SortOrder:  schema.SortAscending,
					Expression: "farm_hash(resource_type_code, resource_external_id)",
				},
				{
					Name:      "resource_type_code",
					Type:      schema.TypeString,
					SortOrder: schema.SortAscending,
					Required:  true,
				},
				{
					Name:      "resource_external_id",
					Type:      schema.TypeString,
					SortOrder: schema.SortAscending,
					Required:  true,
				},
				{
					Name:      "start_time",
					Type:      schema.TypeTimestamp,
					SortOrder: schema.SortAscending,
					Required:  true,
				},
				{
					Name:      "id",
					Type:      schema.TypeString,
					SortOrder: schema.SortAscending,
					Required:  true,
				},
				{
					Name:     "event",
					Type:     schema.TypeBytes,
					Required: true,
				},
			},
		}),
	)
	if err != nil && !yterrors.ContainsErrorCode(err, yterrors.CodeAlreadyExists) {
		panic(err)
	}

	err = yc.ReshardTable(ctx, ypath.Path("//home/events"), &yt.ReshardTableOptions{
		TabletCount: ptr.Int(initialTabletCount),
	})
	if err != nil {
		panic(err)
	}

	_, err = yt.CreateTable(
		ctx,
		yc,
		ypath.Path("//home/events_id_uniq_idx"),
		yt.WithAttributes(map[string]any{
			"dynamic":                  true,
			"optimize_for":             "lookup",
			"in_memory_mode":           "uncompressed",
			"enable_lookup_hash_table": true,
		}),
		yt.WithSchema(schema.Schema{
			UniqueKeys: true,
			Columns: []schema.Column{
				{
					Name:       "hash",
					Type:       schema.TypeUint64,
					SortOrder:  schema.SortAscending,
					Expression: "farm_hash(id)",
				},
				{
					Name:      "id",
					Type:      schema.TypeString,
					SortOrder: schema.SortAscending,
					Required:  true,
				},
				{
					Name:     "resource_type_code",
					Type:     schema.TypeString,
					Required: true,
				},
				{
					Name:     "resource_external_id",
					Type:     schema.TypeString,
					Required: true,
				},
				{
					Name:     "start_time",
					Type:     schema.TypeTimestamp,
					Required: true,
				},
			},
		}),
	)
	if err != nil && !yterrors.ContainsErrorCode(err, yterrors.CodeAlreadyExists) {
		panic(err)
	}

	err = yc.ReshardTable(ctx, ypath.Path("//home/events_id_uniq_idx"), &yt.ReshardTableOptions{
		TabletCount: ptr.Int(initialTabletCount),
	})
	if err != nil {
		panic(err)
	}
}
