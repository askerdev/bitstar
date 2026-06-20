package bitstar

type EventRow struct {
	ResourceTypeCode   string `yson:"resource_type_code"`
	ResourceExternalID string `yson:"resource_external_id"`
	StartTime          uint64 `yson:"start_time"`
	ID                 string `yson:"id"`
	Event              []byte `yson:"event"`
}

type EventRowKey struct {
	ResourceTypeCode   string `yson:"resource_type_code"`
	ResourceExternalID string `yson:"resource_external_id"`
	StartTime          uint64 `yson:"start_time"`
	ID                 string `yson:"id"`
}

type EventIndexRowKey struct {
	ID string `yson:"id"`
}
