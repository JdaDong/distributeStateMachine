package service

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/distributeStateMachine/raft"
	"github.com/distributeStateMachine/statemachine"
)

// =========================================================================
// Mock Proposer
// =========================================================================

// mockProposer 模拟 Raft 节点的 Propose 能力
type mockProposer struct {
	store    *statemachine.KVStore
	isLeader bool
	leaderID string
	proposeErr error
}

func newMockProposer(store *statemachine.KVStore) *mockProposer {
	return &mockProposer{
		store:    store,
		isLeader: true,
		leaderID: "node1",
	}
}

func (m *mockProposer) Propose(command []byte) (raft.ApplyMsg, error) {
	if !m.isLeader {
		return raft.ApplyMsg{}, fmt.Errorf("not leader")
	}
	if m.proposeErr != nil {
		return raft.ApplyMsg{}, m.proposeErr
	}

	// 模拟共识过程：直接 Apply 到状态机
	_, err := m.store.Apply(command)
	if err != nil {
		// Apply 出错仍然返回 ApplyMsg（command 有效但可能被状态机拒绝）
		return raft.ApplyMsg{
			CommandValid: true,
			Command:      command,
			CommandIndex: 1,
			CommandTerm:  1,
		}, nil
	}

	return raft.ApplyMsg{
		CommandValid: true,
		Command:      command,
		CommandIndex: 1,
		CommandTerm:  1,
	}, nil
}

func (m *mockProposer) GetLeaderID() string {
	return m.leaderID
}

// =========================================================================
// 辅助函数
// =========================================================================

func setupTestService() (*TaskService, *mockProposer, *statemachine.KVStore) {
	store := statemachine.NewKVStore()
	proposer := newMockProposer(store)
	svc := NewTaskService(proposer, store)
	return svc, proposer, store
}

// 快速创建一个 PENDING 任务
func createPendingTask(svc *TaskService, taskID string) {
	svc.SetTaskStatus(&SetTaskStatusRequest{
		TaskID: taskID,
		Status: "PENDING",
	})
}

// =========================================================================
// SetTaskStatus 测试
// =========================================================================

func TestTaskService_SetTaskStatus_CreatePending(t *testing.T) {
	svc, _, _ := setupTestService()

	resp, err := svc.SetTaskStatus(&SetTaskStatusRequest{
		TaskID:  "job-001",
		Status:  "PENDING",
		Message: "新建任务",
	})

	if err != nil {
		t.Fatalf("SetTaskStatus 返回错误: %v", err)
	}
	if !resp.Success {
		t.Errorf("创建 PENDING 任务应成功, 错误: %s", resp.Error)
	}
	if resp.Task == nil {
		t.Fatal("成功时应返回 Task")
	}
	if resp.Task.TaskID != "job-001" {
		t.Errorf("TaskID 应为 job-001, 实际为 %s", resp.Task.TaskID)
	}
	if resp.Task.Status != "PENDING" {
		t.Errorf("Status 应为 PENDING, 实际为 %s", resp.Task.Status)
	}
}

func TestTaskService_SetTaskStatus_FullLifecycle(t *testing.T) {
	svc, _, _ := setupTestService()

	// 1. 创建 PENDING
	resp, _ := svc.SetTaskStatus(&SetTaskStatusRequest{
		TaskID: "job-001", Status: "PENDING",
	})
	if !resp.Success {
		t.Fatalf("创建 PENDING 失败: %s", resp.Error)
	}

	// 2. PENDING → RUNNING
	resp, _ = svc.SetTaskStatus(&SetTaskStatusRequest{
		TaskID: "job-001", Status: "RUNNING", Message: "处理中",
	})
	if !resp.Success {
		t.Fatalf("PENDING→RUNNING 失败: %s", resp.Error)
	}
	if resp.Task.Status != "RUNNING" {
		t.Errorf("状态应为 RUNNING, 实际为 %s", resp.Task.Status)
	}

	// 3. RUNNING → SUCCESS
	resp, _ = svc.SetTaskStatus(&SetTaskStatusRequest{
		TaskID: "job-001", Status: "SUCCESS", Message: "完成",
	})
	if !resp.Success {
		t.Fatalf("RUNNING→SUCCESS 失败: %s", resp.Error)
	}
	if resp.Task.Status != "SUCCESS" {
		t.Errorf("状态应为 SUCCESS, 实际为 %s", resp.Task.Status)
	}
}

func TestTaskService_SetTaskStatus_EmptyTaskID(t *testing.T) {
	svc, _, _ := setupTestService()

	resp, err := svc.SetTaskStatus(&SetTaskStatusRequest{
		TaskID: "",
		Status: "PENDING",
	})

	if err != nil {
		t.Fatalf("不应返回 error: %v", err)
	}
	if resp.Success {
		t.Error("空 task_id 应失败")
	}
	if resp.Error != "task_id is required" {
		t.Errorf("错误信息应为 'task_id is required', 实际为 '%s'", resp.Error)
	}
}

func TestTaskService_SetTaskStatus_InvalidStatus(t *testing.T) {
	svc, _, _ := setupTestService()

	invalidStatuses := []string{"INVALID", "running", "Done", "OK", ""}

	for _, status := range invalidStatuses {
		t.Run("status="+status, func(t *testing.T) {
			resp, err := svc.SetTaskStatus(&SetTaskStatusRequest{
				TaskID: "job-001",
				Status: status,
			})

			if err != nil {
				t.Fatalf("不应返回 error: %v", err)
			}
			if resp.Success {
				t.Errorf("无效状态 '%s' 应失败", status)
			}
		})
	}
}

func TestTaskService_SetTaskStatus_AllValidStatuses(t *testing.T) {
	validStatuses := []string{"PENDING", "RUNNING", "SUCCESS", "FAILED", "CANCELLED", "TIMEOUT"}

	for _, status := range validStatuses {
		t.Run("status="+status, func(t *testing.T) {
			svc, _, _ := setupTestService()

			resp, err := svc.SetTaskStatus(&SetTaskStatusRequest{
				TaskID: "job-001",
				Status: status,
			})

			if err != nil {
				t.Fatalf("不应返回 error: %v", err)
			}
			// 首次创建任何状态都应该成功（新任务）
			if !resp.Success {
				t.Errorf("创建 %s 状态的新任务应成功, 错误: %s", status, resp.Error)
			}
		})
	}
}

func TestTaskService_SetTaskStatus_NotLeader(t *testing.T) {
	svc, proposer, _ := setupTestService()
	proposer.isLeader = false

	resp, err := svc.SetTaskStatus(&SetTaskStatusRequest{
		TaskID: "job-001",
		Status: "PENDING",
	})

	if err != nil {
		t.Fatalf("不应返回 error: %v", err)
	}
	if resp.Success {
		t.Error("非 Leader 节点应返回失败")
	}
	if resp.Error != "not leader" {
		t.Errorf("错误信息应为 'not leader', 实际为 '%s'", resp.Error)
	}
}

func TestTaskService_SetTaskStatus_ProposeTimeout(t *testing.T) {
	svc, proposer, _ := setupTestService()
	proposer.proposeErr = fmt.Errorf("proposal timeout")

	resp, err := svc.SetTaskStatus(&SetTaskStatusRequest{
		TaskID: "job-001",
		Status: "PENDING",
	})

	if err != nil {
		t.Fatalf("不应返回 error: %v", err)
	}
	if resp.Success {
		t.Error("Propose 超时应返回失败")
	}
}

func TestTaskService_SetTaskStatus_InvalidTransition(t *testing.T) {
	svc, _, _ := setupTestService()

	// 创建 PENDING 任务
	createPendingTask(svc, "job-001")

	// 尝试非法转换: PENDING → SUCCESS
	resp, err := svc.SetTaskStatus(&SetTaskStatusRequest{
		TaskID: "job-001",
		Status: "SUCCESS",
	})

	if err != nil {
		t.Fatalf("不应返回 error: %v", err)
	}
	if resp.Success {
		t.Error("PENDING→SUCCESS 应被状态机拒绝")
	}
}

func TestTaskService_SetTaskStatus_TerminalImmutable(t *testing.T) {
	svc, _, _ := setupTestService()

	// 创建完整生命周期到 SUCCESS
	svc.SetTaskStatus(&SetTaskStatusRequest{TaskID: "job-001", Status: "PENDING"})
	svc.SetTaskStatus(&SetTaskStatusRequest{TaskID: "job-001", Status: "RUNNING"})
	svc.SetTaskStatus(&SetTaskStatusRequest{TaskID: "job-001", Status: "SUCCESS"})

	// 尝试修改终态
	resp, _ := svc.SetTaskStatus(&SetTaskStatusRequest{
		TaskID: "job-001",
		Status: "RUNNING",
	})
	if resp.Success {
		t.Error("从终态 SUCCESS 不应允许再次转换")
	}
}

// =========================================================================
// GetTask 测试
// =========================================================================

func TestTaskService_GetTask_Exists(t *testing.T) {
	svc, _, _ := setupTestService()

	createPendingTask(svc, "job-001")

	resp, err := svc.GetTask("job-001")
	if err != nil {
		t.Fatalf("GetTask 返回错误: %v", err)
	}
	if !resp.Success {
		t.Errorf("GetTask 应成功, 错误: %s", resp.Error)
	}
	if resp.Task == nil {
		t.Fatal("Task 不应为 nil")
	}
	if resp.Task.TaskID != "job-001" {
		t.Errorf("TaskID 应为 job-001, 实际为 %s", resp.Task.TaskID)
	}
}

func TestTaskService_GetTask_NotFound(t *testing.T) {
	svc, _, _ := setupTestService()

	resp, err := svc.GetTask("nonexistent")
	if err != nil {
		t.Fatalf("GetTask 不应返回 error: %v", err)
	}
	if resp.Success {
		t.Error("查询不存在的任务应失败")
	}
	if resp.Error != "task not found" {
		t.Errorf("错误信息应为 'task not found', 实际为 '%s'", resp.Error)
	}
}

func TestTaskService_GetTask_EmptyID(t *testing.T) {
	svc, _, _ := setupTestService()

	resp, err := svc.GetTask("")
	if err != nil {
		t.Fatalf("GetTask 不应返回 error: %v", err)
	}
	if resp.Success {
		t.Error("空 task_id 应失败")
	}
	if resp.Error != "task_id is required" {
		t.Errorf("错误信息应为 'task_id is required', 实际为 '%s'", resp.Error)
	}
}

// =========================================================================
// ListTasks 测试
// =========================================================================

func TestTaskService_ListTasks_Empty(t *testing.T) {
	svc, _, _ := setupTestService()

	resp, err := svc.ListTasks()
	if err != nil {
		t.Fatalf("ListTasks 返回错误: %v", err)
	}
	if !resp.Success {
		t.Errorf("ListTasks 应成功, 错误: %s", resp.Error)
	}
	if len(resp.Tasks) != 0 {
		t.Errorf("空状态应返回 0 个任务, 实际 %d", len(resp.Tasks))
	}
}

func TestTaskService_ListTasks_Multiple(t *testing.T) {
	svc, _, _ := setupTestService()

	createPendingTask(svc, "job-001")
	createPendingTask(svc, "job-002")
	createPendingTask(svc, "job-003")

	resp, err := svc.ListTasks()
	if err != nil {
		t.Fatalf("ListTasks 返回错误: %v", err)
	}
	if !resp.Success {
		t.Errorf("ListTasks 应成功, 错误: %s", resp.Error)
	}
	if len(resp.Tasks) != 3 {
		t.Errorf("应返回 3 个任务, 实际 %d", len(resp.Tasks))
	}
}

// =========================================================================
// GetLeaderHint 测试
// =========================================================================

func TestTaskService_GetLeaderHint(t *testing.T) {
	svc, proposer, _ := setupTestService()

	hint := svc.GetLeaderHint()
	if hint != "node1" {
		t.Errorf("LeaderHint 应为 node1, 实际为 %s", hint)
	}

	proposer.leaderID = "node2"
	hint = svc.GetLeaderHint()
	if hint != "node2" {
		t.Errorf("LeaderHint 应为 node2, 实际为 %s", hint)
	}

	proposer.leaderID = ""
	hint = svc.GetLeaderHint()
	if hint != "" {
		t.Errorf("无 Leader 时 Hint 应为空, 实际为 %s", hint)
	}
}

// =========================================================================
// isValidStatus 测试
// =========================================================================

func TestIsValidStatus(t *testing.T) {
	valids := []string{"PENDING", "RUNNING", "SUCCESS", "FAILED", "CANCELLED", "TIMEOUT"}
	invalids := []string{"pending", "DONE", "UNKNOWN", "", "running"}

	for _, s := range valids {
		if !isValidStatus(s) {
			t.Errorf("%s 应为合法状态", s)
		}
	}
	for _, s := range invalids {
		if isValidStatus(s) {
			t.Errorf("'%s' 不应为合法状态", s)
		}
	}
}

// =========================================================================
// 请求/响应类型序列化测试
// =========================================================================

func TestSetTaskStatusRequest_JSON(t *testing.T) {
	req := SetTaskStatusRequest{
		TaskID:  "job-001",
		Status:  "RUNNING",
		Message: "处理中",
	}
	data, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("序列化失败: %v", err)
	}

	var decoded SetTaskStatusRequest
	err = json.Unmarshal(data, &decoded)
	if err != nil {
		t.Fatalf("反序列化失败: %v", err)
	}

	if decoded.TaskID != req.TaskID || decoded.Status != req.Status || decoded.Message != req.Message {
		t.Errorf("JSON 往返不一致: got %+v, want %+v", decoded, req)
	}
}

func TestSetTaskStatusResponse_JSON(t *testing.T) {
	resp := SetTaskStatusResponse{
		Success: true,
		Error:   "",
	}
	data, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("序列化失败: %v", err)
	}

	var decoded SetTaskStatusResponse
	json.Unmarshal(data, &decoded)
	if decoded.Success != true {
		t.Error("反序列化后 Success 应为 true")
	}
}
