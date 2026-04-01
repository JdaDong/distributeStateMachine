package raft

import (
	"sync"
	"time"

	pb "github.com/distributeStateMachine/proto"
)

// NodeState 表示 Raft 节点的角色
type NodeState int

const (
	Follower  NodeState = iota // 追随者
	Candidate                  // 候选人
	Leader                     // 领导者
)

func (s NodeState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// Config 是 Raft 节点的配置
type Config struct {
	ID                string            // 当前节点 ID
	Peers             map[string]string // 对等节点：ID -> 地址
	ElectionTimeout   time.Duration     // 选举超时时间
	HeartbeatInterval time.Duration     // 心跳间隔
	ListenAddr        string            // 监听地址
	SnapshotThreshold uint64            // 日志条目数达到此阈值时触发快照（0 表示不自动快照）
}

// LogEntry 表示 Raft 日志条目
type LogEntry struct {
	Term    uint64 // 任期号
	Index   uint64 // 日志索引
	Command []byte // 状态机命令
}

// ToProto 将 LogEntry 转为 protobuf 消息
func (e *LogEntry) ToProto() *pb.LogEntry {
	return &pb.LogEntry{
		Term:    e.Term,
		Index:   e.Index,
		Command: e.Command,
	}
}

// LogEntryFromProto 从 protobuf 消息创建 LogEntry
func LogEntryFromProto(p *pb.LogEntry) LogEntry {
	return LogEntry{
		Term:    p.Term,
		Index:   p.Index,
		Command: p.Command,
	}
}

// PersistentState 是需要持久化的 Raft 状态
type PersistentState struct {
	CurrentTerm uint64     `json:"current_term"` // 当前任期号
	VotedFor    string     `json:"voted_for"`    // 当前任期投票给了谁
	Log         []LogEntry `json:"log"`          // 日志条目（快照之后的部分）

	// 快照截断点：日志压缩后，SnapshotIndex 之前的日志已被快照替代
	SnapshotIndex uint64 `json:"snapshot_index"` // 快照包含的最后一条日志索引
	SnapshotTerm  uint64 `json:"snapshot_term"`  // 快照包含的最后一条日志任期
}

// VolatileState 是不需要持久化的 Raft 状态
type VolatileState struct {
	CommitIndex uint64 // 已知已提交的最大日志索引
	LastApplied uint64 // 已应用到状态机的最大日志索引
}

// LeaderState 是 Leader 独有的状态
type LeaderState struct {
	NextIndex  map[string]uint64 // 每个节点下一条待发送的日志索引
	MatchIndex map[string]uint64 // 每个节点已知已复制的最高日志索引
}

// ApplyMsg 是提交给状态机应用的消息
type ApplyMsg struct {
	CommandValid bool
	Command      []byte
	CommandIndex uint64
	CommandTerm  uint64
}

// pendingProposal 追踪客户端提案的完成状态
type pendingProposal struct {
	index  uint64
	term   uint64
	result chan ApplyMsg
}

// RaftNode 是 Raft 共识节点的核心结构
type RaftNode struct {
	mu sync.RWMutex

	// 配置
	config Config

	// 当前角色
	state NodeState

	// 持久化状态
	persistent PersistentState

	// 易失性状态
	volatile VolatileState

	// Leader 状态
	leaderState LeaderState

	// 通道
	applyCh     chan ApplyMsg  // 已提交日志发送到状态机
	stopCh      chan struct{}  // 停止信号
	resetTimerCh chan struct{} // 重置选举定时器

	// 待处理的客户端提案
	pendingProposals map[uint64]*pendingProposal

	// RPC 客户端（与其他节点通信）
	transport Transport

	// 持久化存储
	storage Storage

	// 状态机
	stateMachine StateMachine
}
