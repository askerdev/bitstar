package bitstar

import (
	"encoding/binary"
	"time"

	"github.com/google/uuid"
)

func encodeKey(startTime time.Time, id uuid.UUID) []byte {
	buf := make([]byte, 24)
	encodeTime(buf[0:8], startTime)
	copy(buf[8:24], id[:])
	return buf
}

func encodeTime(dst []byte, t time.Time) {
	binary.BigEndian.PutUint64(dst, uint64(t.UnixNano()))
}

func decodeKey(key []byte) (time.Time, uuid.UUID) {
	nanos := binary.BigEndian.Uint64(key[0:8])
	startTime := time.Unix(0, int64(nanos))

	id, err := uuid.FromBytes(key[8:24])
	if err != nil {
		panic(err)
	}

	return startTime, id
}
