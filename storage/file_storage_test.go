package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/distributeStateMachine/raft"
)

func setupTestStorage(t *testing.T) (*FileStorage, string) {
	t.Helper()
	dir := t.TempDir()
	store, err := NewFileStorage(dir)
	if err != nil {
		t.Fatalf("创建 FileStorage 失败: %v", err)
	}
	return store, dir
}

func TestFileStorage_NewCreatesDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "subdir", "nested")
	store, err := NewFileStorage(dir)
	if err != nil {
		t.Fatalf("创建 FileStorage 失败: %v", err)
	}
	if store == nil {
		t.Fatal("store 不应为 nil")
	}

	info, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("目录应已创建: %v", err)
	}
	if !info.IsDir() {
		t.Error("路径应为目录")
	}
}

func TestFileStorage_LoadEmptyState(t *testing.T) {
	store, _ := setupTestStorage(t)

	state, err := store.LoadState()
	if err != nil {
		t.Fatalf("LoadState 不应报错: %v", err)
	}
	if state != nil {
		t.Error("不存在文件时 LoadState 应返回 nil")
	}
}

func TestFileStorage_SaveAndLoad(t *testing.T) {
	store, _ := setupTestStorage(t)

	original := &raft.PersistentState{
		CurrentTerm: 5,
		VotedFor:    "node2",
		Log: []raft.LogEntry{
			{Term: 1, Index: 1, Command: []byte("cmd1")},
			{Term: 3, Index: 2, Command: []byte("cmd2")},
			{Term: 5, Index: 3, Command: []byte("cmd3")},
		},
	}

	err := store.SaveState(original)
	if err != nil {
		t.Fatalf("SaveState 失败: %v", err)
	}

	loaded, err := store.LoadState()
	if err != nil {
		t.Fatalf("LoadState 失败: %v", err)
	}
	if loaded == nil {
		t.Fatal("LoadState 不应返回 nil")
	}

	if loaded.CurrentTerm != original.CurrentTerm {
		t.Errorf("CurrentTerm: got %d, want %d", loaded.CurrentTerm, original.CurrentTerm)
	}
	if loaded.VotedFor != original.VotedFor {
		t.Errorf("VotedFor: got %s, want %s", loaded.VotedFor, original.VotedFor)
	}
	if len(loaded.Log) != len(original.Log) {
		t.Fatalf("Log 长度: got %d, want %d", len(loaded.Log), len(original.Log))
	}
	for i, entry := range loaded.Log {
		if entry.Term != original.Log[i].Term {
			t.Errorf("Log[%d].Term: got %d, want %d", i, entry.Term, original.Log[i].Term)
		}
		if entry.Index != original.Log[i].Index {
			t.Errorf("Log[%d].Index: got %d, want %d", i, entry.Index, original.Log[i].Index)
		}
		if string(entry.Command) != string(original.Log[i].Command) {
			t.Errorf("Log[%d].Command: got %s, want %s", i, entry.Command, original.Log[i].Command)
		}
	}
}

func TestFileStorage_OverwriteState(t *testing.T) {
	store, _ := setupTestStorage(t)

	// 第一次保存
	state1 := &raft.PersistentState{
		CurrentTerm: 1,
		VotedFor:    "node1",
		Log:         []raft.LogEntry{{Term: 1, Index: 1, Command: []byte("old")}},
	}
	store.SaveState(state1)

	// 第二次保存（覆盖）
	state2 := &raft.PersistentState{
		CurrentTerm: 10,
		VotedFor:    "node3",
		Log:         []raft.LogEntry{{Term: 10, Index: 1, Command: []byte("new")}},
	}
	store.SaveState(state2)

	loaded, _ := store.LoadState()
	if loaded.CurrentTerm != 10 {
		t.Errorf("覆盖后 CurrentTerm 应为 10, 实际为 %d", loaded.CurrentTerm)
	}
	if loaded.VotedFor != "node3" {
		t.Errorf("覆盖后 VotedFor 应为 node3, 实际为 %s", loaded.VotedFor)
	}
	if string(loaded.Log[0].Command) != "new" {
		t.Errorf("覆盖后命令应为 new, 实际为 %s", loaded.Log[0].Command)
	}
}

func TestFileStorage_SaveEmptyLog(t *testing.T) {
	store, _ := setupTestStorage(t)

	state := &raft.PersistentState{
		CurrentTerm: 0,
		VotedFor:    "",
		Log:         []raft.LogEntry{},
	}

	err := store.SaveState(state)
	if err != nil {
		t.Fatalf("保存空日志状态失败: %v", err)
	}

	loaded, err := store.LoadState()
	if err != nil {
		t.Fatalf("加载空日志状态失败: %v", err)
	}
	if len(loaded.Log) != 0 {
		t.Errorf("空日志应保持为空, 实际长度 %d", len(loaded.Log))
	}
}

func TestFileStorage_AtomicWrite(t *testing.T) {
	store, dir := setupTestStorage(t)

	state := &raft.PersistentState{
		CurrentTerm: 1,
		VotedFor:    "node1",
		Log:         nil,
	}
	store.SaveState(state)

	// 验证没有残留的 .tmp 文件
	tmpPath := filepath.Join(dir, "raft_state.json.tmp")
	_, err := os.Stat(tmpPath)
	if err == nil {
		t.Error("保存成功后不应存在 .tmp 文件")
	}

	// 验证主文件存在
	mainPath := filepath.Join(dir, "raft_state.json")
	_, err = os.Stat(mainPath)
	if err != nil {
		t.Errorf("主文件应存在: %v", err)
	}
}

func TestFileStorage_Close(t *testing.T) {
	store, _ := setupTestStorage(t)
	err := store.Close()
	if err != nil {
		t.Errorf("Close 不应返回错误: %v", err)
	}
}

func TestFileStorage_LargeLog(t *testing.T) {
	store, _ := setupTestStorage(t)

	log := make([]raft.LogEntry, 1000)
	for i := 0; i < 1000; i++ {
		log[i] = raft.LogEntry{
			Term:    uint64(i/100 + 1),
			Index:   uint64(i + 1),
			Command: []byte("command-data"),
		}
	}

	state := &raft.PersistentState{
		CurrentTerm: 10,
		VotedFor:    "node5",
		Log:         log,
	}

	err := store.SaveState(state)
	if err != nil {
		t.Fatalf("保存大日志失败: %v", err)
	}

	loaded, err := store.LoadState()
	if err != nil {
		t.Fatalf("加载大日志失败: %v", err)
	}
	if len(loaded.Log) != 1000 {
		t.Errorf("大日志长度应为 1000, 实际为 %d", len(loaded.Log))
	}
}

// =========================================================================
// 快照持久化测试
// =========================================================================

func TestFileStorage_LoadSnapshotEmpty(t *testing.T) {
	store, _ := setupTestStorage(t)

	meta, data, err := store.LoadSnapshot()
	if err != nil {
		t.Fatalf("LoadSnapshot 不应报错: %v", err)
	}
	if meta != nil {
		t.Error("无快照文件时 meta 应为 nil")
	}
	if data != nil {
		t.Error("无快照文件时 data 应为 nil")
	}
}

func TestFileStorage_SaveAndLoadSnapshot(t *testing.T) {
	store, _ := setupTestStorage(t)

	metadata := raft.SnapshotMetadata{
		LastIncludedIndex: 42,
		LastIncludedTerm:  5,
		CreatedAt:         "2026-01-01T00:00:00Z",
	}
	snapData := []byte(`{"data":{"key1":"val1"},"tasks":{"job-1":{"task_id":"job-1","status":"RUNNING"}}}`)

	err := store.SaveSnapshot(metadata, snapData)
	if err != nil {
		t.Fatalf("SaveSnapshot 失败: %v", err)
	}

	loadedMeta, loadedData, err := store.LoadSnapshot()
	if err != nil {
		t.Fatalf("LoadSnapshot 失败: %v", err)
	}
	if loadedMeta == nil {
		t.Fatal("loadedMeta 不应为 nil")
	}

	if loadedMeta.LastIncludedIndex != 42 {
		t.Errorf("LastIncludedIndex 应为 42, 实际为 %d", loadedMeta.LastIncludedIndex)
	}
	if loadedMeta.LastIncludedTerm != 5 {
		t.Errorf("LastIncludedTerm 应为 5, 实际为 %d", loadedMeta.LastIncludedTerm)
	}
	if loadedMeta.CreatedAt != "2026-01-01T00:00:00Z" {
		t.Errorf("CreatedAt 不匹配: %s", loadedMeta.CreatedAt)
	}

	if string(loadedData) != string(snapData) {
		t.Errorf("快照数据不一致\n  want: %s\n  got:  %s", snapData, loadedData)
	}
}

func TestFileStorage_OverwriteSnapshot(t *testing.T) {
	store, _ := setupTestStorage(t)

	// 第一次保存
	meta1 := raft.SnapshotMetadata{LastIncludedIndex: 10, LastIncludedTerm: 1}
	store.SaveSnapshot(meta1, []byte("old-snapshot"))

	// 第二次保存（覆盖）
	meta2 := raft.SnapshotMetadata{LastIncludedIndex: 50, LastIncludedTerm: 3}
	store.SaveSnapshot(meta2, []byte("new-snapshot"))

	loadedMeta, loadedData, _ := store.LoadSnapshot()
	if loadedMeta.LastIncludedIndex != 50 {
		t.Errorf("覆盖后 LastIncludedIndex 应为 50, 实际为 %d", loadedMeta.LastIncludedIndex)
	}
	if string(loadedData) != "new-snapshot" {
		t.Errorf("覆盖后数据应为 new-snapshot, 实际为 %s", loadedData)
	}
}

func TestFileStorage_SnapshotAtomicWrite(t *testing.T) {
	store, dir := setupTestStorage(t)

	meta := raft.SnapshotMetadata{LastIncludedIndex: 10, LastIncludedTerm: 1}
	store.SaveSnapshot(meta, []byte("snapshot-data"))

	// 验证没有残留的 .tmp 文件
	tmpPath := filepath.Join(dir, "snapshot.json.tmp")
	_, err := os.Stat(tmpPath)
	if err == nil {
		t.Error("保存成功后不应存在 snapshot.json.tmp 文件")
	}

	// 验证主文件存在
	mainPath := filepath.Join(dir, "snapshot.json")
	_, err = os.Stat(mainPath)
	if err != nil {
		t.Errorf("snapshot.json 应存在: %v", err)
	}
}

func TestFileStorage_SaveStateWithSnapshotFields(t *testing.T) {
	store, _ := setupTestStorage(t)

	state := &raft.PersistentState{
		CurrentTerm:   5,
		VotedFor:      "node1",
		SnapshotIndex: 20,
		SnapshotTerm:  3,
		Log: []raft.LogEntry{
			{Term: 4, Index: 21, Command: []byte("cmd21")},
			{Term: 5, Index: 22, Command: []byte("cmd22")},
		},
	}

	store.SaveState(state)
	loaded, _ := store.LoadState()

	if loaded.SnapshotIndex != 20 {
		t.Errorf("SnapshotIndex 应为 20, 实际为 %d", loaded.SnapshotIndex)
	}
	if loaded.SnapshotTerm != 3 {
		t.Errorf("SnapshotTerm 应为 3, 实际为 %d", loaded.SnapshotTerm)
	}
	if len(loaded.Log) != 2 {
		t.Errorf("Log 应有 2 条, 实际为 %d", len(loaded.Log))
	}
	if loaded.Log[0].Index != 21 {
		t.Errorf("第一条日志 Index 应为 21, 实际为 %d", loaded.Log[0].Index)
	}
}

func TestFileStorage_LargeSnapshot(t *testing.T) {
	store, _ := setupTestStorage(t)

	// 1MB 的快照数据
	bigData := make([]byte, 1024*1024)
	for i := range bigData {
		bigData[i] = byte(i % 256)
	}

	meta := raft.SnapshotMetadata{LastIncludedIndex: 100, LastIncludedTerm: 10}
	err := store.SaveSnapshot(meta, bigData)
	if err != nil {
		t.Fatalf("保存大快照失败: %v", err)
	}

	loadedMeta, loadedData, err := store.LoadSnapshot()
	if err != nil {
		t.Fatalf("加载大快照失败: %v", err)
	}
	if loadedMeta.LastIncludedIndex != 100 {
		t.Errorf("LastIncludedIndex 应为 100, 实际为 %d", loadedMeta.LastIncludedIndex)
	}
	if len(loadedData) != len(bigData) {
		t.Errorf("大快照数据长度应为 %d, 实际为 %d", len(bigData), len(loadedData))
	}
}
