package storage

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/distributeStateMachine/raft"
)

// FileStorage 基于文件的持久化存储
type FileStorage struct {
	mu           sync.Mutex
	dir          string
	filePath     string // raft_state.json — Raft 日志和元数据
	snapshotPath string // snapshot.json — 状态机快照
}

// snapshotFile 快照文件的 JSON 结构
type snapshotFile struct {
	Metadata raft.SnapshotMetadata `json:"metadata"`
	Data     []byte                `json:"data"` // 状态机序列化数据
}

// NewFileStorage 创建文件存储
func NewFileStorage(dir string) (*FileStorage, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	return &FileStorage{
		dir:          dir,
		filePath:     filepath.Join(dir, "raft_state.json"),
		snapshotPath: filepath.Join(dir, "snapshot.json"),
	}, nil
}

// SaveState 保存 Raft 持久化状态到文件
func (fs *FileStorage) SaveState(state *raft.PersistentState) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	data, err := json.Marshal(state)
	if err != nil {
		return err
	}

	// 先写入临时文件，再原子重命名，保证写入安全
	tmpPath := fs.filePath + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmpPath, fs.filePath)
}

// LoadState 从文件加载 Raft 持久化状态
func (fs *FileStorage) LoadState() (*raft.PersistentState, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	data, err := os.ReadFile(fs.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var state raft.PersistentState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}
	return &state, nil
}

// SaveSnapshot 保存状态机快照到磁盘
// 使用原子写入（先写临时文件再 rename）保证崩溃安全
func (fs *FileStorage) SaveSnapshot(metadata raft.SnapshotMetadata, data []byte) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	sf := snapshotFile{
		Metadata: metadata,
		Data:     data,
	}

	jsonData, err := json.Marshal(sf)
	if err != nil {
		return fmt.Errorf("marshal snapshot failed: %w", err)
	}

	// 原子写入
	tmpPath := fs.snapshotPath + ".tmp"
	if err := os.WriteFile(tmpPath, jsonData, 0644); err != nil {
		return fmt.Errorf("write snapshot tmp file failed: %w", err)
	}
	if err := os.Rename(tmpPath, fs.snapshotPath); err != nil {
		return fmt.Errorf("rename snapshot file failed: %w", err)
	}

	log.Printf("[FileStorage] 快照已保存: index=%d, term=%d, size=%d bytes",
		metadata.LastIncludedIndex, metadata.LastIncludedTerm, len(data))
	return nil
}

// LoadSnapshot 从磁盘加载最新的状态机快照
// 如果没有快照文件，返回 nil, nil, nil
func (fs *FileStorage) LoadSnapshot() (*raft.SnapshotMetadata, []byte, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	data, err := os.ReadFile(fs.snapshotPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, nil
		}
		return nil, nil, fmt.Errorf("read snapshot file failed: %w", err)
	}

	var sf snapshotFile
	if err := json.Unmarshal(data, &sf); err != nil {
		return nil, nil, fmt.Errorf("unmarshal snapshot failed: %w", err)
	}

	log.Printf("[FileStorage] 快照已加载: index=%d, term=%d, size=%d bytes",
		sf.Metadata.LastIncludedIndex, sf.Metadata.LastIncludedTerm, len(sf.Data))
	return &sf.Metadata, sf.Data, nil
}

// Close 关闭存储（文件存储无需特殊关闭操作）
func (fs *FileStorage) Close() error {
	return nil
}
