package bitstar

import (
	"encoding/base64"

	storagepb "github.com/askerdev/bitstar/proto/infralenta/storage/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func DecodePageToken(pageToken string) (EventKey, error) {
	bytes, err := base64.StdEncoding.DecodeString(pageToken)
	if err != nil {
		return EventKey{}, err
	}
	pbKey := &storagepb.EventKey{}
	if err := proto.Unmarshal(bytes, pbKey); err != nil {
		return EventKey{}, err
	}
	return EventKey{
		ResourceTypeCode:   pbKey.GetResource().GetTypeCode(),
		ResourceExternalID: pbKey.GetResource().GetExternalId(),
		StartTime:          pbKey.GetStartTime().AsTime(),
		ID:                 pbKey.GetId(),
	}, nil
}

func EncodePageToken(eventKey EventKey) (string, error) {
	bytes, err := proto.Marshal(&storagepb.EventKey{
		Resource: &storagepb.Resource{
			TypeCode:   eventKey.ResourceTypeCode,
			ExternalId: eventKey.ResourceExternalID,
		},
		StartTime: timestamppb.New(eventKey.StartTime),
		Id:        eventKey.ID,
	})
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(bytes), nil
}
