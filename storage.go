package bitstar

import (
	"context"

	storagepb "github.com/askerdev/bitstar/proto/infralenta/storage/v1"
	"github.com/google/uuid"
	"go.etcd.io/bbolt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

var (
	eventsBucket = []byte("events")
)

type Storage struct {
	db     *bbolt.DB
	levels [4]*roaringIndex
}

func Open(path string) (*Storage, error) {
	db, err := bbolt.Open(path, 0600, nil)
	if err != nil {
		return nil, err
	}

	err = db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(eventsBucket)
		return err
	})
	if err != nil {
		return nil, err
	}

	ri, err := fromBbolt(db, eventsBucket)
	if err != nil {
		return nil, err
	}

	return &Storage{
		db:     db,
		levels: [4]*roaringIndex{nil, nil, nil, ri},
	}, nil
}

func (s *Storage) Close() error {
	return s.db.Close()
}

func (s *Storage) BatchCreateEvents(ctx context.Context, in *storagepb.BatchCreateEventsRequest) (*storagepb.BatchCreateEventsResponse, error) {
	events := []*storagepb.Event{}

	err := s.db.Batch(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(eventsBucket)

		for _, request := range in.GetRequests() {
			id := uuid.Must(uuid.NewV7())
			request.Event.Id = id.String()

			key := encodeKey(request.Event.StartTime.AsTime(), id)

			val, err := proto.Marshal(request.Event)
			if err != nil {
				return err
			}

			if err := bucket.Put(key, val); err != nil {
				return err
			}

			events = append(events, request.Event)
		}

		return nil
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "batch create fail: %v", err)
	}

	return &storagepb.BatchCreateEventsResponse{
		Events: events,
	}, nil
}

func (s *Storage) ListEvents(ctx context.Context, in *storagepb.ListEventsRequest) (*storagepb.ListEventsResponse, error) {
	q := Query{
		PageToken: in.GetPageToken(),
		PageSize:  int(in.GetPageSize()),
		StartTime: in.GetStartTime().AsTime(),
		EndTime:   in.GetEndTime().AsTime(),
		Filter:    in.GetFilter(),
	}

	keys, err := q.Do(s.levels[:])
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list fail: %v", err)
	}

	events := make([]*storagepb.Event, 0, len(keys))

	err = s.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(eventsBucket)
		for _, key := range keys {
			event := &storagepb.Event{}
			if err := proto.Unmarshal(bucket.Get(key), event); err != nil {
				return err
			}
			events = append(events, event)
		}
		return nil
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list fail: %v", err)
	}

	return &storagepb.ListEventsResponse{
		Events:        events,
		NextPageToken: q.NextPageToken(),
	}, nil
}
