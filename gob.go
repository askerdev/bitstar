package bitstar

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"time"

	"github.com/google/uuid"
)

func encodeEvent(event *Event) []byte {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(event); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func decodeEvent(data []byte, event *Event) {
	buf := bytes.NewBuffer(data)
	if err := gob.NewDecoder(buf).Decode(event); err != nil {
		panic(err)
	}
}

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
	nanos := binary.BigEndian.Uint64(key[16:24])
	startTime := time.Unix(0, int64(nanos))
	buf := make([]byte, 16)
	copy(buf, key[0:16])
	id, err := uuid.FromBytes(buf)
	if err != nil {
		panic(err)
	}
	return startTime, id
}
