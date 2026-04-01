package raft

import (
	"context"

	pb "github.com/distributeStateMachine/proto"
)

// Transport 定义节点间通信接口
type Transport interface {
	// SendRequestVote 发送投票请求
	SendRequestVote(ctx context.Context, target string, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error)

	// SendAppendEntries 发送追加日志/心跳请求
	SendAppendEntries(ctx context.Context, target string, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error)

	// Start 启动传输层
	Start() error

	// Stop 停止传输层
	Stop() error
}

// Storage 定义持久化存储接口
type Storage interface {
	// SaveState 保存 Raft 持久化状态
	SaveState(state *PersistentState) error

	// LoadState 加载 Raft 持久化状态
	LoadState() (*PersistentState, error)

	// SaveSnapshot 保存状态机快照到磁盘
	// metadata 包含快照对应的日志索引和任期，data 是状态机序列化数据
	SaveSnapshot(metadata SnapshotMetadata, data []byte) error

	// LoadSnapshot 从磁盘加载最新的状态机快照
	// 返回快照元数据和状态机数据；如果没有快照返回 nil, nil, nil
	LoadSnapshot() (*SnapshotMetadata, []byte, error)

	// Close 关闭存储
	Close() error
}

// StateMachine 定义状态机接口
type StateMachine interface {
	// Apply 将命令应用到状态机
	Apply(command []byte) ([]byte, error)

	// Snapshot 获取状态机快照（用于日志压缩）
	Snapshot() ([]byte, error)

	// Restore 从快照恢复状态机
	Restore(snapshot []byte) error
}

// SnapshotMetadata 快照元数据
type SnapshotMetadata struct {
	// 快照包含的最后一条日志的索引
	LastIncludedIndex uint64 `json:"last_included_index"`
	// 快照包含的最后一条日志的任期
	LastIncludedTerm uint64 `json:"last_included_term"`
	// 快照创建时间
	CreatedAt string `json:"created_at"`
}
