package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

const (
	WALMagicNumber = uint32(0x57414C21) // "WAL!"
	WALVersion     = uint32(1)

	EntryTypeData = uint8(1)

	WALFileHeaderSize = 8
	EntryHeaderSize   = 9

	DefaultMaxEntrySize   = 10 * 1024 * 1024  // 10MB
	DefaultMaxSegmentSize = 100 * 1024 * 1024 // 100MB
)

var (
	ErrCorruptedWAL  = errors.New("WAL file is corrupted")
	ErrInvalidEntry  = errors.New("invalid entry format")
	ErrEntryTooLarge = errors.New("entry exceeds maximum size")
	ErrWALClosed     = errors.New("WAL is closed")
)

type WALEntry struct {
	Type     uint8
	Data     []byte
	Checksum uint32
}

type EntryIndex struct {
	Index  uint64
	Offset int64
}

type WALMetrics struct {
	WriteCount   int64
	SyncCount    int64
	BytesWritten int64
	Corruptions  int64
	LastSyncTime int64
}

type Config struct {
	MaxEntrySize   uint32
	MaxSegmentSize int64
}

type WAL struct {
	file     *os.File
	filePath string
	dirPath  string

	writeMu sync.Mutex
	readMu  sync.RWMutex
	indexMu sync.RWMutex

	index     []EntryIndex
	nextIndex uint64

	config  *Config
	offset  int64
	closed  atomic.Int32
	metrics WALMetrics
}

func Must(w *WAL, err error) *WAL {
	if err != nil {
		panic(err)
	}
	return w
}

func New(filePath string) (*WAL, error) {
	return NewWithConfig(filePath, &Config{
		MaxEntrySize:   DefaultMaxEntrySize,
		MaxSegmentSize: DefaultMaxSegmentSize,
	})
}

func NewWithConfig(filePath string, config *Config) (*WAL, error) {
	dirPath := filepath.Dir(filePath)
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return nil, err
	}

	dir, _ := os.Open(dirPath)
	dir.Sync()
	dir.Close()

	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	w := &WAL{
		file:      file,
		filePath:  filePath,
		dirPath:   dirPath,
		config:    config,
		index:     make([]EntryIndex, 0),
		nextIndex: 1,
	}

	if err := w.initialize(); err != nil {
		file.Close()
		return nil, err
	}
	return w, nil
}

func (w *WAL) Append(data []byte) error {
	if w.closed.Load() == 1 {
		return ErrWALClosed
	}
	if data == nil {
		return fmt.Errorf("data is nil")
	}
	if uint32(len(data)) > w.config.MaxEntrySize {
		return ErrEntryTooLarge
	}

	w.writeMu.Lock()
	defer w.writeMu.Unlock()

	entry := &WALEntry{Type: EntryTypeData, Data: data}
	entry.Checksum = computeChecksum(entry.Type, data)
	encoded := entry.encode()

	n, err := w.file.Write(encoded)
	if err != nil {
		return err
	}

	entryOffset := w.offset
	w.offset += int64(n)

	w.indexMu.Lock()
	w.index = append(w.index, EntryIndex{Index: w.nextIndex, Offset: entryOffset})
	w.indexMu.Unlock()

	w.nextIndex++
	atomic.AddInt64(&w.metrics.WriteCount, 1)
	atomic.AddInt64(&w.metrics.BytesWritten, int64(n))
	return nil
}

func (w *WAL) Sync() error {
	w.writeMu.Lock()
	defer w.writeMu.Unlock()
	err := w.file.Sync()
	atomic.AddInt64(&w.metrics.SyncCount, 1)
	atomic.StoreInt64(&w.metrics.LastSyncTime, time.Now().UnixNano())
	return err
}

func (w *WAL) GetEntry(index uint64) ([]byte, error) {
	w.indexMu.RLock()
	if index == 0 || index > uint64(len(w.index)) {
		w.indexMu.RUnlock()
		return nil, fmt.Errorf("index out of bounds")
	}
	info := w.index[index-1]
	w.indexMu.RUnlock()

	w.readMu.RLock()
	defer w.readMu.RUnlock()
	entry, _, err := w.readEntryAt(info.Offset)
	return entry.Data, err
}

func (w *WAL) AppendAndSync(data []byte) error {
	if err := w.Append(data); err != nil {
		return err
	}
	return w.Sync()
}

func (w *WAL) LastIndex() uint64 {
	w.indexMu.RLock()
	defer w.indexMu.RUnlock()
	if len(w.index) == 0 {
		return 0
	}
	return w.index[len(w.index)-1].Index
}

func (w *WAL) ReadAll() ([][]byte, error) {
	w.indexMu.RLock()
	indices := make([]EntryIndex, len(w.index))
	copy(indices, w.index)
	w.indexMu.RUnlock()

	results := make([][]byte, 0, len(indices))
	w.readMu.RLock()
	defer w.readMu.RUnlock()

	for _, idx := range indices {
		entry, _, err := w.readEntryAt(idx.Offset)
		if err != nil {
			return nil, fmt.Errorf("failed to read entry at index %d: %w", idx.Index, err)
		}
		results = append(results, entry.Data)
	}

	return results, nil
}

func (w *WAL) Close() error {
	if !w.closed.CompareAndSwap(0, 1) {
		return nil
	}
	w.Sync()
	return w.file.Close()
}

func (w *WAL) initialize() error {
	stat, _ := w.file.Stat()
	if stat.Size() == 0 {
		buf := make([]byte, WALFileHeaderSize)
		binary.BigEndian.PutUint32(buf[0:4], WALMagicNumber)
		binary.BigEndian.PutUint32(buf[4:8], WALVersion)
		w.file.Write(buf)
		w.file.Sync()
		w.offset = int64(WALFileHeaderSize)
		return nil
	}
	return w.recover()
}

func (w *WAL) recover() error {
	header := make([]byte, WALFileHeaderSize)
	if _, err := w.file.ReadAt(header, 0); err != nil {
		return err
	}
	if binary.BigEndian.Uint32(header[0:4]) != WALMagicNumber {
		return ErrCorruptedWAL
	}

	offset := int64(WALFileHeaderSize)
	nextIdx := uint64(1)

	for {
		_, size, err := w.readEntryAt(offset)
		if err != nil {
			if err != io.EOF {
				w.truncate(offset)
			}
			break
		}
		w.index = append(w.index, EntryIndex{Index: nextIdx, Offset: offset})
		offset += size
		nextIdx++
	}
	w.offset = offset
	w.nextIndex = nextIdx
	w.file.Seek(w.offset, 0)
	return nil
}

func (w *WAL) readEntryAt(offset int64) (*WALEntry, int64, error) {
	headBuf := make([]byte, EntryHeaderSize)
	if _, err := w.file.ReadAt(headBuf, offset); err != nil {
		return nil, 0, err
	}

	dLen := binary.BigEndian.Uint32(headBuf[1:5])
	if dLen > w.config.MaxEntrySize {
		return nil, 0, ErrEntryTooLarge
	}

	data := make([]byte, dLen)
	if _, err := w.file.ReadAt(data, offset+EntryHeaderSize); err != nil {
		return nil, 0, err
	}

	entry := &WALEntry{Type: headBuf[0], Data: data, Checksum: binary.BigEndian.Uint32(headBuf[5:9])}
	if computeChecksum(entry.Type, data) != entry.Checksum {
		atomic.AddInt64(&w.metrics.Corruptions, 1)
		return nil, 0, ErrCorruptedWAL
	}
	return entry, int64(EntryHeaderSize + dLen), nil
}

func (w *WAL) truncate(offset int64) error {
	if err := w.file.Truncate(offset); err != nil {
		return err
	}
	return w.file.Sync()
}

// TruncateFromIndex removes all entries from the given index onwards.
// index is 1-based. If index is 5, entries 5, 6, 7... are deleted.
// This is essential for Raft when a follower must resolve log conflicts.
func (w *WAL) TruncateFromIndex(index uint64) error {
	if w.closed.Load() == 1 {
		return ErrWALClosed
	}

	w.writeMu.Lock()
	defer w.writeMu.Unlock()

	w.indexMu.Lock()
	defer w.indexMu.Unlock()

	// 1. Validation: Ensure index is within the current log range
	if index == 0 || index > uint64(len(w.index)) {
		return fmt.Errorf("invalid truncate index: %d (current log size: %d)", index, len(w.index))
	}

	// 2. Find the file offset of the entry to be removed
	// Since index is 1-based, index-1 is the slice position.
	truncateOffset := w.index[index-1].Offset

	// 3. Physical Truncation
	// This removes the data from the underlying storage.
	if err := w.file.Truncate(truncateOffset); err != nil {
		return fmt.Errorf("failed to physically truncate file: %w", err)
	}

	// 4. Force Sync
	// Critical: Ensure the file system metadata (new size) is durable.
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync after truncation: %w", err)
	}

	// 5. Update In-Memory State
	w.index = w.index[:index-1] // Remove indices from memory
	w.nextIndex = index         // Set next index to the one we just cleared
	w.offset = truncateOffset   // Move write pointer back

	// 6. Reset File Pointer
	// Required because Append uses w.file.Write()
	if _, err := w.file.Seek(w.offset, 0); err != nil {
		return fmt.Errorf("failed to seek to new end: %w", err)
	}

	return nil
}

func (e *WALEntry) encode() []byte {
	dLen := uint32(len(e.Data))
	buf := make([]byte, EntryHeaderSize+dLen)
	buf[0] = e.Type
	binary.BigEndian.PutUint32(buf[1:5], dLen)
	binary.BigEndian.PutUint32(buf[5:9], e.Checksum)
	copy(buf[9:], e.Data)
	return buf
}

func computeChecksum(t uint8, data []byte) uint32 {
	crc := crc32.NewIEEE()
	var header [5]byte
	header[0] = t
	binary.BigEndian.PutUint32(header[1:5], uint32(len(data)))
	crc.Write(header[:])
	crc.Write(data)
	return crc.Sum32()
}
