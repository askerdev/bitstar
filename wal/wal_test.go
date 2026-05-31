package wal

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
)

func TestNew(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	w, err := New(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer w.Close()

	if w == nil {
		t.Fatal("WAL is nil")
	}

	// Verify WAL is functional by checking LastIndex
	if w.LastIndex() != 0 {
		t.Errorf("Expected LastIndex to be 0 for new WAL, got %d", w.LastIndex())
	}
}

func TestNewWithConfig(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	config := &Config{
		MaxEntrySize:   1024,
		MaxSegmentSize: 10240,
	}

	w, err := NewWithConfig(walPath, config)
	if err != nil {
		t.Fatalf("Failed to create WAL with config: %v", err)
	}
	defer w.Close()

	// Test that config is applied by trying to append data larger than MaxEntrySize
	largeData := make([]byte, 1025)
	err = w.Append(largeData)
	if err != ErrEntryTooLarge {
		t.Errorf("Expected ErrEntryTooLarge, got %v", err)
	}
}

func TestAppend(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	w, err := New(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer w.Close()

	data := []byte("test data")
	err = w.Append(data)
	if err != nil {
		t.Fatalf("Failed to append: %v", err)
	}

	if w.LastIndex() != 1 {
		t.Errorf("Expected LastIndex to be 1, got %d", w.LastIndex())
	}

	// Verify entry can be retrieved
	retrieved, err := w.GetEntry(1)
	if err != nil {
		t.Fatalf("Failed to get entry: %v", err)
	}
	if !reflect.DeepEqual(retrieved, data) {
		t.Errorf("Retrieved data doesn't match: expected %s, got %s", string(data), string(retrieved))
	}
}

func TestAppendMultiple(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	w, err := New(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer w.Close()

	entries := [][]byte{
		[]byte("entry 1"),
		[]byte("entry 2"),
		[]byte("entry 3"),
	}

	for i, entry := range entries {
		err := w.Append(entry)
		if err != nil {
			t.Fatalf("Failed to append entry %d: %v", i+1, err)
		}
	}

	if w.LastIndex() != 3 {
		t.Errorf("Expected LastIndex to be 3, got %d", w.LastIndex())
	}

	if len(w.index) != 3 {
		t.Errorf("Expected index length to be 3, got %d", len(w.index))
	}
}

func TestAppendNilData(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	w, err := New(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer w.Close()

	err = w.Append(nil)
	if err == nil {
		t.Fatal("Expected error when appending nil data")
	}
}

func TestAppendTooLarge(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	config := &Config{
		MaxEntrySize:   100,
		MaxSegmentSize: 1000,
	}

	w, err := NewWithConfig(walPath, config)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer w.Close()

	largeData := make([]byte, 101)
	err = w.Append(largeData)
	if err != ErrEntryTooLarge {
		t.Errorf("Expected ErrEntryTooLarge, got %v", err)
	}
}

func TestAppendAfterClose(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	w, err := New(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	w.Close()

	err = w.Append([]byte("test"))
	if err != ErrWALClosed {
		t.Errorf("Expected ErrWALClosed, got %v", err)
	}
}

func TestSync(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	w, err := New(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer w.Close()

	err = w.Append([]byte("test data"))
	if err != nil {
		t.Fatalf("Failed to append: %v", err)
	}

	err = w.Sync()
	if err != nil {
		t.Fatalf("Failed to sync: %v", err)
	}

	if w.metrics.SyncCount != 1 {
		t.Errorf("Expected SyncCount to be 1, got %d", w.metrics.SyncCount)
	}
}

func TestAppendAndSync(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	w, err := New(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer w.Close()

	data := []byte("test data")
	err = w.AppendAndSync(data)
	if err != nil {
		t.Fatalf("Failed to append and sync: %v", err)
	}

	if w.metrics.WriteCount != 1 {
		t.Errorf("Expected WriteCount to be 1, got %d", w.metrics.WriteCount)
	}

	if w.metrics.SyncCount != 1 {
		t.Errorf("Expected SyncCount to be 1, got %d", w.metrics.SyncCount)
	}
}

func TestGetEntry(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	w, err := New(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer w.Close()

	entries := [][]byte{
		[]byte("entry 1"),
		[]byte("entry 2"),
		[]byte("entry 3"),
	}

	for _, entry := range entries {
		err := w.Append(entry)
		if err != nil {
			t.Fatalf("Failed to append: %v", err)
		}
	}

	for i, expected := range entries {
		actual, err := w.GetEntry(uint64(i + 1))
		if err != nil {
			t.Fatalf("Failed to get entry %d: %v", i+1, err)
		}

		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("Entry %d: expected %s, got %s", i+1, string(expected), string(actual))
		}
	}
}

func TestGetEntryInvalidIndex(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	w, err := New(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer w.Close()

	_, err = w.GetEntry(0)
	if err == nil {
		t.Fatal("Expected error for index 0")
	}

	_, err = w.GetEntry(1)
	if err == nil {
		t.Fatal("Expected error for index 1 when no entries exist")
	}

	w.Append([]byte("test"))
	_, err = w.GetEntry(2)
	if err == nil {
		t.Fatal("Expected error for index out of bounds")
	}
}

func TestReadAll(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	w, err := New(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer w.Close()

	entries := [][]byte{
		[]byte("entry 1"),
		[]byte("entry 2"),
		[]byte("entry 3"),
	}

	for _, entry := range entries {
		err := w.Append(entry)
		if err != nil {
			t.Fatalf("Failed to append: %v", err)
		}
	}

	all, err := w.ReadAll()
	if err != nil {
		t.Fatalf("Failed to read all: %v", err)
	}

	if len(all) != len(entries) {
		t.Errorf("Expected %d entries, got %d", len(entries), len(all))
	}

	for i, expected := range entries {
		if !reflect.DeepEqual(all[i], expected) {
			t.Errorf("Entry %d: expected %s, got %s", i+1, string(expected), string(all[i]))
		}
	}
}

func TestReadAllEmpty(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	w, err := New(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer w.Close()

	all, err := w.ReadAll()
	if err != nil {
		t.Fatalf("Failed to read all: %v", err)
	}

	if len(all) != 0 {
		t.Errorf("Expected 0 entries, got %d", len(all))
	}
}

func TestLastIndex(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	w, err := New(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer w.Close()

	if w.LastIndex() != 0 {
		t.Errorf("Expected LastIndex to be 0 for empty WAL, got %d", w.LastIndex())
	}

	w.Append([]byte("entry 1"))
	if w.LastIndex() != 1 {
		t.Errorf("Expected LastIndex to be 1, got %d", w.LastIndex())
	}

	w.Append([]byte("entry 2"))
	if w.LastIndex() != 2 {
		t.Errorf("Expected LastIndex to be 2, got %d", w.LastIndex())
	}
}

func TestRecovery(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	// Create WAL and write entries
	w1, err := New(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	entries := [][]byte{
		[]byte("entry 1"),
		[]byte("entry 2"),
		[]byte("entry 3"),
	}

	for _, entry := range entries {
		err := w1.AppendAndSync(entry)
		if err != nil {
			t.Fatalf("Failed to append: %v", err)
		}
	}

	w1.Close()

	// Reopen and recover
	w2, err := New(walPath)
	if err != nil {
		t.Fatalf("Failed to recover WAL: %v", err)
	}
	defer w2.Close()

	if w2.LastIndex() != 3 {
		t.Errorf("Expected LastIndex to be 3 after recovery, got %d", w2.LastIndex())
	}

	recovered, err := w2.ReadAll()
	if err != nil {
		t.Fatalf("Failed to read all: %v", err)
	}

	if len(recovered) != len(entries) {
		t.Errorf("Expected %d entries after recovery, got %d", len(entries), len(recovered))
	}

	for i, expected := range entries {
		if !reflect.DeepEqual(recovered[i], expected) {
			t.Errorf("Entry %d: expected %s, got %s", i+1, string(expected), string(recovered[i]))
		}
	}
}

func TestTruncateFromIndex(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	w, err := New(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer w.Close()

	entries := [][]byte{
		[]byte("entry 1"),
		[]byte("entry 2"),
		[]byte("entry 3"),
		[]byte("entry 4"),
		[]byte("entry 5"),
	}

	for _, entry := range entries {
		err := w.AppendAndSync(entry)
		if err != nil {
			t.Fatalf("Failed to append: %v", err)
		}
	}

	// Truncate from index 3 (should keep entries 1 and 2)
	err = w.TruncateFromIndex(3)
	if err != nil {
		t.Fatalf("Failed to truncate: %v", err)
	}

	if w.LastIndex() != 2 {
		t.Errorf("Expected LastIndex to be 2 after truncation, got %d", w.LastIndex())
	}

	remaining, err := w.ReadAll()
	if err != nil {
		t.Fatalf("Failed to read all: %v", err)
	}

	if len(remaining) != 2 {
		t.Errorf("Expected 2 entries after truncation, got %d", len(remaining))
	}

	if !reflect.DeepEqual(remaining[0], entries[0]) {
		t.Errorf("Expected first entry to be %s, got %s", string(entries[0]), string(remaining[0]))
	}

	if !reflect.DeepEqual(remaining[1], entries[1]) {
		t.Errorf("Expected second entry to be %s, got %s", string(entries[1]), string(remaining[1]))
	}
}

func TestTruncateFromIndexInvalid(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	w, err := New(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer w.Close()

	// Truncate with no entries
	err = w.TruncateFromIndex(1)
	if err == nil {
		t.Fatal("Expected error when truncating empty WAL")
	}

	w.Append([]byte("entry 1"))

	// Truncate with index 0
	err = w.TruncateFromIndex(0)
	if err == nil {
		t.Fatal("Expected error when truncating with index 0")
	}

	// Truncate with index out of bounds
	err = w.TruncateFromIndex(10)
	if err == nil {
		t.Fatal("Expected error when truncating with out of bounds index")
	}
}

func TestTruncateFromIndexAfterClose(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	w, err := New(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	w.Append([]byte("entry 1"))
	w.Close()

	err = w.TruncateFromIndex(1)
	if err != ErrWALClosed {
		t.Errorf("Expected ErrWALClosed, got %v", err)
	}
}

func TestTruncateFromIndexAndRecovery(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	// Create WAL and write entries
	w1, err := New(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	entries := [][]byte{
		[]byte("entry 1"),
		[]byte("entry 2"),
		[]byte("entry 3"),
	}

	for _, entry := range entries {
		err := w1.AppendAndSync(entry)
		if err != nil {
			t.Fatalf("Failed to append: %v", err)
		}
	}

	// Truncate
	err = w1.TruncateFromIndex(2)
	if err != nil {
		t.Fatalf("Failed to truncate: %v", err)
	}

	w1.Close()

	// Recover and verify truncation persisted
	w2, err := New(walPath)
	if err != nil {
		t.Fatalf("Failed to recover WAL: %v", err)
	}
	defer w2.Close()

	if w2.LastIndex() != 1 {
		t.Errorf("Expected LastIndex to be 1 after recovery, got %d", w2.LastIndex())
	}

	recovered, err := w2.ReadAll()
	if err != nil {
		t.Fatalf("Failed to read all: %v", err)
	}

	if len(recovered) != 1 {
		t.Errorf("Expected 1 entry after recovery, got %d", len(recovered))
	}
}

func TestConcurrentAppends(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	w, err := New(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer w.Close()

	var wg sync.WaitGroup
	numGoroutines := 10
	entriesPerGoroutine := 10

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < entriesPerGoroutine; j++ {
				data := []byte{byte(id), byte(j)}
				err := w.Append(data)
				if err != nil {
					t.Errorf("Failed to append in goroutine %d: %v", id, err)
				}
			}
		}(i)
	}

	wg.Wait()

	// Verify all entries were written
	all, err := w.ReadAll()
	if err != nil {
		t.Fatalf("Failed to read all: %v", err)
	}

	expectedCount := numGoroutines * entriesPerGoroutine
	if len(all) != expectedCount {
		t.Errorf("Expected %d entries, got %d", expectedCount, len(all))
	}
}

func TestMetrics(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	w, err := New(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer w.Close()

	// Test that operations complete successfully (metrics are internal)
	data := []byte("test data")
	err = w.Append(data)
	if err != nil {
		t.Fatalf("Failed to append: %v", err)
	}

	err = w.Sync()
	if err != nil {
		t.Fatalf("Failed to sync: %v", err)
	}

	// Verify data persists after sync
	retrieved, err := w.GetEntry(1)
	if err != nil {
		t.Fatalf("Failed to get entry: %v", err)
	}
	if !reflect.DeepEqual(retrieved, data) {
		t.Errorf("Data doesn't match after sync")
	}
}

func TestClose(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	w, err := New(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}

	err = w.Append([]byte("test"))
	if err != nil {
		t.Fatalf("Failed to append: %v", err)
	}

	err = w.Close()
	if err != nil {
		t.Fatalf("Failed to close: %v", err)
	}

	// Closing again should be safe
	err = w.Close()
	if err != nil {
		t.Fatalf("Failed to close second time: %v", err)
	}
}

func TestEmptyData(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	w, err := New(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer w.Close()

	// Empty slice should be valid
	err = w.Append([]byte{})
	if err != nil {
		t.Fatalf("Failed to append empty data: %v", err)
	}

	entry, err := w.GetEntry(1)
	if err != nil {
		t.Fatalf("Failed to get entry: %v", err)
	}

	if len(entry) != 0 {
		t.Errorf("Expected empty entry, got %d bytes", len(entry))
	}
}

func TestLargeData(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	config := &Config{
		MaxEntrySize:   1024 * 1024, // 1MB
		MaxSegmentSize: 10 * 1024 * 1024,
	}

	w, err := NewWithConfig(walPath, config)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer w.Close()

	largeData := make([]byte, 512*1024) // 512KB
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	err = w.Append(largeData)
	if err != nil {
		t.Fatalf("Failed to append large data: %v", err)
	}

	recovered, err := w.GetEntry(1)
	if err != nil {
		t.Fatalf("Failed to get entry: %v", err)
	}

	if len(recovered) != len(largeData) {
		t.Errorf("Expected %d bytes, got %d", len(largeData), len(recovered))
	}

	if !reflect.DeepEqual(recovered, largeData) {
		t.Error("Recovered data doesn't match original")
	}
}

func TestFileHeader(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	w, err := New(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	w.Close()

	// Read file directly to verify header
	file, err := os.Open(walPath)
	if err != nil {
		t.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	header := make([]byte, WALFileHeaderSize)
	_, err = file.Read(header)
	if err != nil {
		t.Fatalf("Failed to read header: %v", err)
	}

	if len(header) != WALFileHeaderSize {
		t.Errorf("Expected header size %d, got %d", WALFileHeaderSize, len(header))
	}

	// Verify magic number
	magic := binary.BigEndian.Uint32(header[0:4])
	if magic != WALMagicNumber {
		t.Errorf("Expected magic number 0x%X, got 0x%X", WALMagicNumber, magic)
	}

	// Verify version
	version := binary.BigEndian.Uint32(header[4:8])
	if version != WALVersion {
		t.Errorf("Expected version %d, got %d", WALVersion, version)
	}
}
