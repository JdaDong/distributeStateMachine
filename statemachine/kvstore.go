package statemachine

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/distributeStateMachine/raft"
)

// KVStore 是基于内存的键值存储状态机
// 同时承载 KV 数据和 Task 数据，类比区块链的世界状态（World State）
type KVStore struct {
	mu    sync.RWMutex
	data  map[string]string // 通用 KV 数据
	tasks map[string]*Task  // 任务状态（World State 中的 Task 部分）
}

// Task 任务模型（状态机内部维护的状态）
type Task struct {
	TaskID      string `json:"task_id"`
	Status      string `json:"status"`
	Message     string `json:"message,omitempty"`
	CreatedAt   string `json:"created_at"`
	UpdatedAt   string `json:"updated_at"`
	CompletedAt string `json:"completed_at,omitempty"`
}

// NewKVStore 创建一个新的 KV 存储状态机
func NewKVStore() *KVStore {
	return &KVStore{
		data:  make(map[string]string),
		tasks: make(map[string]*Task),
	}
}

// kvSnapshot 用于序列化快照
type kvSnapshot struct {
	Data  map[string]string `json:"data"`
	Tasks map[string]*Task  `json:"tasks"`
}

// =========================================================================
// Apply：共识达成后，所有节点一致地执行交易
// 类比区块链：矿工/验证者执行交易，更新世界状态
// =========================================================================

// Apply 将交易应用到状态机
// 这是唯一改变状态的入口，由 Raft 共识保证所有节点以相同顺序执行
func (kv *KVStore) Apply(command []byte) ([]byte, error) {
	// 解析交易
	tx, err := raft.DecodeTx(command)
	if err != nil {
		// 兼容旧格式的 KV 命令
		return kv.applyLegacyCommand(command)
	}

	switch tx.TxType {
	case raft.TxTypeKV:
		return kv.applyKVTx(tx.Payload)
	case raft.TxTypeTask:
		return kv.applyTaskTx(tx.Payload)
	default:
		return nil, fmt.Errorf("unknown transaction type: %s", tx.TxType)
	}
}

// applyLegacyCommand 兼容旧格式 KV 命令
func (kv *KVStore) applyLegacyCommand(command []byte) ([]byte, error) {
	cmd, err := raft.DecodeCommand(command)
	if err != nil {
		return nil, fmt.Errorf("decode command failed: %w", err)
	}
	return kv.executeKVOp(cmd)
}

// applyKVTx 执行 KV 交易
func (kv *KVStore) applyKVTx(payload json.RawMessage) ([]byte, error) {
	var cmd raft.Command
	if err := json.Unmarshal(payload, &cmd); err != nil {
		return nil, fmt.Errorf("decode KV payload failed: %w", err)
	}
	return kv.executeKVOp(cmd)
}

// executeKVOp 执行具体的 KV 操作
func (kv *KVStore) executeKVOp(cmd raft.Command) ([]byte, error) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch cmd.Op {
	case "SET":
		kv.data[cmd.Key] = cmd.Value
		log.Printf("[KVStore] SET %s = %s", cmd.Key, cmd.Value)
		return []byte("OK"), nil
	case "DELETE":
		delete(kv.data, cmd.Key)
		log.Printf("[KVStore] DELETE %s", cmd.Key)
		return []byte("OK"), nil
	default:
		return nil, fmt.Errorf("unknown KV operation: %s", cmd.Op)
	}
}

// =========================================================================
// Task 交易执行：所有校验和状态变更都在这里完成
// 类比区块链：智能合约执行逻辑
// =========================================================================

// applyTaskTx 执行 Task 交易
// 关键：状态校验在这里做，而不是在提交交易之前做
// 因为只有 Apply 是在共识达成后、所有节点一致执行的
func (kv *KVStore) applyTaskTx(payload json.RawMessage) ([]byte, error) {
	var taskTx raft.TaskTransaction
	if err := json.Unmarshal(payload, &taskTx); err != nil {
		return nil, fmt.Errorf("decode Task payload failed: %w", err)
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	now := time.Now().Format(time.RFC3339)

	existing, exists := kv.tasks[taskTx.TaskID]
	if exists {
		// === 状态转换校验（在 Apply 内做，保证一致性） ===
		if err := validateTransition(existing.Status, taskTx.Status); err != nil {
			// 交易被拒绝（类比区块链中的交易 revert）
			result := ApplyResult{
				Success: false,
				Error:   err.Error(),
			}
			return json.Marshal(result)
		}

		// 校验通过，更新状态
		existing.Status = taskTx.Status
		existing.Message = taskTx.Message
		existing.UpdatedAt = now
		if isTerminalStatus(taskTx.Status) {
			existing.CompletedAt = now
		}

		log.Printf("[StateMachine] TASK_TX: %s 状态变更 → %s", taskTx.TaskID, taskTx.Status)

		result := ApplyResult{Success: true, Task: existing}
		return json.Marshal(result)
	}

	// 新任务：创建
	task := &Task{
		TaskID:    taskTx.TaskID,
		Status:    taskTx.Status,
		Message:   taskTx.Message,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if isTerminalStatus(taskTx.Status) {
		task.CompletedAt = now
	}

	kv.tasks[taskTx.TaskID] = task
	log.Printf("[StateMachine] TASK_TX: 创建任务 %s, 状态: %s", taskTx.TaskID, taskTx.Status)

	result := ApplyResult{Success: true, Task: task}
	return json.Marshal(result)
}

// ApplyResult Task 交易的执行结果
type ApplyResult struct {
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
	Task    *Task  `json:"task,omitempty"`
}

// =========================================================================
// 状态转换规则（类比智能合约中的业务逻辑）
// =========================================================================

// 合法的状态转换表
var allowedTransitions = map[string][]string{
	"PENDING": {"RUNNING", "CANCELLED"},
	"RUNNING": {"SUCCESS", "FAILED", "CANCELLED", "TIMEOUT"},
}

// 终态集合
var terminalStatuses = map[string]bool{
	"SUCCESS":   true,
	"FAILED":    true,
	"CANCELLED": true,
	"TIMEOUT":   true,
}

// validateTransition 校验状态转换是否合法
func validateTransition(from, to string) error {
	if terminalStatuses[from] {
		return fmt.Errorf("cannot transition from terminal status %s to %s", from, to)
	}

	allowed, ok := allowedTransitions[from]
	if !ok {
		return fmt.Errorf("no transitions defined from status %s", from)
	}

	for _, a := range allowed {
		if a == to {
			return nil
		}
	}

	return fmt.Errorf("invalid transition: %s -> %s", from, to)
}

func isTerminalStatus(status string) bool {
	return terminalStatuses[status]
}

// =========================================================================
// 读取方法（不经过 Raft，直接读本地状态）
// =========================================================================

// Get 从状态机读取 KV 值
func (kv *KVStore) Get(key string) (string, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	val, ok := kv.data[key]
	return val, ok
}

// GetTask 从状态机读取任务
func (kv *KVStore) GetTask(taskID string) (*Task, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	task, ok := kv.tasks[taskID]
	if !ok {
		return nil, false
	}
	// 返回副本
	copy := *task
	return &copy, true
}

// GetAllTasks 获取所有任务
func (kv *KVStore) GetAllTasks() []*Task {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	tasks := make([]*Task, 0, len(kv.tasks))
	for _, t := range kv.tasks {
		copy := *t
		tasks = append(tasks, &copy)
	}
	return tasks
}

// Snapshot 获取状态机快照
func (kv *KVStore) Snapshot() ([]byte, error) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	snap := kvSnapshot{
		Data:  make(map[string]string, len(kv.data)),
		Tasks: make(map[string]*Task, len(kv.tasks)),
	}
	for k, v := range kv.data {
		snap.Data[k] = v
	}
	for k, v := range kv.tasks {
		copy := *v
		snap.Tasks[k] = &copy
	}
	return json.Marshal(snap)
}

// Restore 从快照恢复状态机
func (kv *KVStore) Restore(snapshot []byte) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	var snap kvSnapshot
	if err := json.Unmarshal(snapshot, &snap); err != nil {
		return err
	}
	kv.data = snap.Data
	if snap.Tasks != nil {
		kv.tasks = snap.Tasks
	} else {
		kv.tasks = make(map[string]*Task)
	}
	return nil
}

// GetAll 获取所有键值对（用于调试）
func (kv *KVStore) GetAll() map[string]string {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	result := make(map[string]string, len(kv.data))
	for k, v := range kv.data {
		result[k] = v
	}
	// 也把 task 数据序列化进来（方便 /api/status 查看）
	for id, t := range kv.tasks {
		if data, err := json.Marshal(t); err == nil {
			result["task:"+id] = string(data)
		}
	}
	return result
}
