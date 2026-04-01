package service

import (
	"encoding/json"
	"fmt"

	"github.com/distributeStateMachine/raft"
	"github.com/distributeStateMachine/statemachine"
)

// =========================================================================
// 请求/响应类型（HTTP API 层使用）
// =========================================================================

// SetTaskStatusRequest 设置任务状态的请求
type SetTaskStatusRequest struct {
	TaskID  string `json:"task_id"`
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

// SetTaskStatusResponse 设置任务状态的响应
type SetTaskStatusResponse struct {
	Success bool              `json:"success"`
	Task    *statemachine.Task `json:"task,omitempty"`
	Error   string            `json:"error,omitempty"`
}

// GetTaskResponse 获取任务的响应
type GetTaskResponse struct {
	Success bool              `json:"success"`
	Task    *statemachine.Task `json:"task,omitempty"`
	Error   string            `json:"error,omitempty"`
}

// ListTasksResponse 列出所有任务的响应
type ListTasksResponse struct {
	Success bool                `json:"success"`
	Tasks   []*statemachine.Task `json:"tasks"`
	Error   string              `json:"error,omitempty"`
}

// =========================================================================
// Proposer 接口：解耦 Raft 节点的具体实现
// =========================================================================

// Proposer 定义提交交易到 Raft 共识的能力
// 类比区块链：客户端向节点提交交易
type Proposer interface {
	// Propose 提交交易到 Raft 共识，返回共识结果
	Propose(command []byte) (raft.ApplyMsg, error)
	// GetLeaderID 获取当前 Leader ID（用于重定向提示）
	GetLeaderID() string
}

// =========================================================================
// TaskService：交易构建层
// 类比区块链：客户端 SDK，负责构建交易 → 提交给节点
// =========================================================================

// TaskService 任务服务
// 职责：
//   1. 接收业务请求
//   2. 构建 Task 交易（Transaction）
//   3. 提交给本地 Raft 节点 Propose（进入共识流程）
//   4. 等待共识结果，返回给调用方
//
// 注意：状态校验和状态变更不在这里做，而是在状态机 Apply 中做
// 因为只有 Apply 是在共识达成后、所有节点一致执行的
type TaskService struct {
	proposer Proposer                // Raft 节点（通过接口解耦）
	store    *statemachine.KVStore   // 状态机（用于本地读取）
}

// NewTaskService 创建任务服务
func NewTaskService(proposer Proposer, store *statemachine.KVStore) *TaskService {
	return &TaskService{
		proposer: proposer,
		store:    store,
	}
}

// SetTaskStatus 设置任务状态
//
// 工作流程（类比区块链交易）：
//   1. 基本参数校验（格式校验，不涉及状态）
//   2. 构建 TaskTransaction（交易）
//   3. 编码并提交到 Raft Propose（进入共识）
//   4. Raft 日志复制到多数节点 → 共识达成
//   5. 各节点状态机 Apply 执行交易（校验状态转换 + 变更状态）
//   6. Leader 的 Propose 返回执行结果
func (s *TaskService) SetTaskStatus(req *SetTaskStatusRequest) (*SetTaskStatusResponse, error) {
	// 1. 基本参数校验（不涉及状态读取，只是格式检查）
	if req.TaskID == "" {
		return &SetTaskStatusResponse{Success: false, Error: "task_id is required"}, nil
	}
	if !isValidStatus(req.Status) {
		return &SetTaskStatusResponse{
			Success: false,
			Error:   fmt.Sprintf("invalid status: %s, valid: PENDING, RUNNING, SUCCESS, FAILED, CANCELLED, TIMEOUT", req.Status),
		}, nil
	}

	// 2. 构建交易
	taskTx := raft.TaskTransaction{
		TaskID:  req.TaskID,
		Status:  req.Status,
		Message: req.Message,
	}
	payload, err := json.Marshal(taskTx)
	if err != nil {
		return nil, fmt.Errorf("marshal task tx failed: %w", err)
	}

	tx := raft.Transaction{
		TxType:  raft.TxTypeTask,
		Payload: payload,
	}
	txBytes, err := raft.EncodeTx(tx)
	if err != nil {
		return nil, fmt.Errorf("encode tx failed: %w", err)
	}

	// 3. 提交给 Raft 共识（Propose）
	//    Propose 会等待日志复制到多数节点、Apply 执行完成后返回
	applyMsg, err := s.proposer.Propose(txBytes)
	if err != nil {
		errMsg := err.Error()
		if errMsg == "not leader" {
			return &SetTaskStatusResponse{
				Success: false,
				Error:   "not leader",
			}, nil
		}
		return &SetTaskStatusResponse{
			Success: false,
			Error:   fmt.Sprintf("propose failed: %s", errMsg),
		}, nil
	}

	// 4. 解析状态机 Apply 的执行结果
	if applyMsg.Command == nil {
		return &SetTaskStatusResponse{Success: false, Error: "empty apply result"}, nil
	}

	// Apply 返回的 result 是序列化的 ApplyResult
	// 注意：applyMsg.Command 是原始 command，Apply 的返回值需要从状态机获取
	// 由于当前 Propose 返回的是 ApplyMsg（包含 command），
	// 我们需要从状态机读取最新状态来确认结果
	task, exists := s.store.GetTask(req.TaskID)
	if !exists {
		// Apply 可能校验失败导致交易被拒绝
		return &SetTaskStatusResponse{
			Success: false,
			Error:   "transaction may have been rejected by state machine",
		}, nil
	}

	// 检查状态是否符合预期（Apply 可能因校验失败而未变更）
	if task.Status != req.Status {
		return &SetTaskStatusResponse{
			Success: false,
			Error:   fmt.Sprintf("state transition rejected: current status is %s", task.Status),
		}, nil
	}

	return &SetTaskStatusResponse{
		Success: true,
		Task:    task,
	}, nil
}

// GetTask 获取任务详情（读取本地状态机，不经过 Raft 共识）
// 注意：这是"最终一致性"读取，Follower 可能有轻微延迟
func (s *TaskService) GetTask(taskID string) (*GetTaskResponse, error) {
	if taskID == "" {
		return &GetTaskResponse{Success: false, Error: "task_id is required"}, nil
	}

	task, exists := s.store.GetTask(taskID)
	if !exists {
		return &GetTaskResponse{Success: false, Error: "task not found"}, nil
	}

	return &GetTaskResponse{Success: true, Task: task}, nil
}

// ListTasks 列出所有任务（读取本地状态机）
func (s *TaskService) ListTasks() (*ListTasksResponse, error) {
	tasks := s.store.GetAllTasks()
	return &ListTasksResponse{Success: true, Tasks: tasks}, nil
}

// GetLeaderHint 返回 Leader 提示信息
func (s *TaskService) GetLeaderHint() string {
	return s.proposer.GetLeaderID()
}

// =========================================================================
// 辅助方法
// =========================================================================

var validStatuses = map[string]bool{
	"PENDING":   true,
	"RUNNING":   true,
	"SUCCESS":   true,
	"FAILED":    true,
	"CANCELLED": true,
	"TIMEOUT":   true,
}

func isValidStatus(status string) bool {
	return validStatuses[status]
}
