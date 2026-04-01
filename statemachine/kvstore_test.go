package statemachine

import (
	"encoding/json"
	"testing"

	"github.com/distributeStateMachine/raft"
)

func TestKVStore_SetAndGet(t *testing.T) {
	kv := NewKVStore()

	cmd := raft.Command{Op: "SET", Key: "name", Value: "Alice"}
	data, _ := raft.EncodeCommand(cmd)

	result, err := kv.Apply(data)
	if err != nil {
		t.Fatalf("Apply SET 失败: %v", err)
	}
	if string(result) != "OK" {
		t.Errorf("Apply 结果应为 OK, 实际为 %s", result)
	}

	val, ok := kv.Get("name")
	if !ok {
		t.Fatal("Get 应找到 key 'name'")
	}
	if val != "Alice" {
		t.Errorf("Get('name') 应为 Alice, 实际为 %s", val)
	}
}

func TestKVStore_Delete(t *testing.T) {
	kv := NewKVStore()

	// 先设置
	setCmd := raft.Command{Op: "SET", Key: "city", Value: "Beijing"}
	data, _ := raft.EncodeCommand(setCmd)
	kv.Apply(data)

	// 确认存在
	val, ok := kv.Get("city")
	if !ok || val != "Beijing" {
		t.Fatal("SET 后 Get 应能找到值")
	}

	// 删除
	delCmd := raft.Command{Op: "DELETE", Key: "city"}
	data, _ = raft.EncodeCommand(delCmd)
	result, err := kv.Apply(data)
	if err != nil {
		t.Fatalf("Apply DELETE 失败: %v", err)
	}
	if string(result) != "OK" {
		t.Errorf("DELETE 结果应为 OK, 实际为 %s", result)
	}

	// 确认已删除
	_, ok = kv.Get("city")
	if ok {
		t.Error("DELETE 后 Get 不应找到值")
	}
}

func TestKVStore_GetNonExistent(t *testing.T) {
	kv := NewKVStore()

	_, ok := kv.Get("nonexistent")
	if ok {
		t.Error("查询不存在的 key 应返回 false")
	}
}

func TestKVStore_Overwrite(t *testing.T) {
	kv := NewKVStore()

	cmd1 := raft.Command{Op: "SET", Key: "key1", Value: "value1"}
	data, _ := raft.EncodeCommand(cmd1)
	kv.Apply(data)

	cmd2 := raft.Command{Op: "SET", Key: "key1", Value: "value2"}
	data, _ = raft.EncodeCommand(cmd2)
	kv.Apply(data)

	val, ok := kv.Get("key1")
	if !ok {
		t.Fatal("覆盖写入后应能找到 key")
	}
	if val != "value2" {
		t.Errorf("覆盖后值应为 value2, 实际为 %s", val)
	}
}

func TestKVStore_UnknownOperation(t *testing.T) {
	kv := NewKVStore()

	cmd := raft.Command{Op: "UNKNOWN", Key: "key", Value: "val"}
	data, _ := raft.EncodeCommand(cmd)

	_, err := kv.Apply(data)
	if err == nil {
		t.Error("未知操作应返回错误")
	}
}

func TestKVStore_InvalidCommand(t *testing.T) {
	kv := NewKVStore()

	_, err := kv.Apply([]byte("not valid json"))
	if err == nil {
		t.Error("无效命令数据应返回错误")
	}
}

func TestKVStore_SnapshotAndRestore(t *testing.T) {
	kv1 := NewKVStore()

	// 写入多条数据
	cmds := []raft.Command{
		{Op: "SET", Key: "k1", Value: "v1"},
		{Op: "SET", Key: "k2", Value: "v2"},
		{Op: "SET", Key: "k3", Value: "v3"},
	}
	for _, cmd := range cmds {
		data, _ := raft.EncodeCommand(cmd)
		kv1.Apply(data)
	}

	// 创建快照
	snapshot, err := kv1.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot 失败: %v", err)
	}
	if len(snapshot) == 0 {
		t.Fatal("快照数据不应为空")
	}

	// 用快照恢复到新的 KVStore
	kv2 := NewKVStore()
	err = kv2.Restore(snapshot)
	if err != nil {
		t.Fatalf("Restore 失败: %v", err)
	}

	// 验证数据一致性
	for _, cmd := range cmds {
		val, ok := kv2.Get(cmd.Key)
		if !ok {
			t.Errorf("恢复后找不到 key '%s'", cmd.Key)
			continue
		}
		if val != cmd.Value {
			t.Errorf("恢复后 key '%s' 的值应为 '%s', 实际为 '%s'", cmd.Key, cmd.Value, val)
		}
	}
}

func TestKVStore_RestoreInvalidData(t *testing.T) {
	kv := NewKVStore()
	err := kv.Restore([]byte("invalid json"))
	if err == nil {
		t.Error("无效快照数据应返回错误")
	}
}

func TestKVStore_GetAll(t *testing.T) {
	kv := NewKVStore()

	cmds := []raft.Command{
		{Op: "SET", Key: "a", Value: "1"},
		{Op: "SET", Key: "b", Value: "2"},
		{Op: "SET", Key: "c", Value: "3"},
	}
	for _, cmd := range cmds {
		data, _ := raft.EncodeCommand(cmd)
		kv.Apply(data)
	}

	all := kv.GetAll()
	if len(all) != 3 {
		t.Errorf("GetAll 应返回 3 个键值对, 实际返回 %d", len(all))
	}

	expected := map[string]string{"a": "1", "b": "2", "c": "3"}
	for k, v := range expected {
		if all[k] != v {
			t.Errorf("GetAll()[%s] 应为 %s, 实际为 %s", k, v, all[k])
		}
	}

	// 修改返回值不应影响原始数据
	all["a"] = "modified"
	val, _ := kv.Get("a")
	if val != "1" {
		t.Error("GetAll 返回的应是副本，修改不应影响原始数据")
	}
}

func TestKVStore_DeleteNonExistent(t *testing.T) {
	kv := NewKVStore()

	cmd := raft.Command{Op: "DELETE", Key: "nokey"}
	data, _ := raft.EncodeCommand(cmd)
	result, err := kv.Apply(data)
	if err != nil {
		t.Fatalf("删除不存在的 key 不应报错: %v", err)
	}
	if string(result) != "OK" {
		t.Errorf("结果应为 OK, 实际为 %s", result)
	}
}

// =========================================================================
// Task 交易测试
// =========================================================================

// 辅助函数：构建 Task 交易字节
func buildTaskTx(taskID, status, message string) []byte {
	taskTx := raft.TaskTransaction{
		TaskID:  taskID,
		Status:  status,
		Message: message,
	}
	payload, _ := json.Marshal(taskTx)
	tx := raft.Transaction{
		TxType:  raft.TxTypeTask,
		Payload: payload,
	}
	data, _ := raft.EncodeTx(tx)
	return data
}

func TestKVStore_TaskCreate(t *testing.T) {
	kv := NewKVStore()

	data := buildTaskTx("job-001", "PENDING", "初始化任务")
	result, err := kv.Apply(data)
	if err != nil {
		t.Fatalf("创建任务失败: %v", err)
	}

	var applyResult ApplyResult
	if err := json.Unmarshal(result, &applyResult); err != nil {
		t.Fatalf("解析 ApplyResult 失败: %v", err)
	}
	if !applyResult.Success {
		t.Errorf("创建任务应成功, 错误: %s", applyResult.Error)
	}
	if applyResult.Task == nil {
		t.Fatal("ApplyResult.Task 不应为 nil")
	}
	if applyResult.Task.TaskID != "job-001" {
		t.Errorf("TaskID 应为 job-001, 实际为 %s", applyResult.Task.TaskID)
	}
	if applyResult.Task.Status != "PENDING" {
		t.Errorf("Status 应为 PENDING, 实际为 %s", applyResult.Task.Status)
	}
	if applyResult.Task.Message != "初始化任务" {
		t.Errorf("Message 应为 '初始化任务', 实际为 '%s'", applyResult.Task.Message)
	}

	// 验证通过 GetTask 也能读到
	task, ok := kv.GetTask("job-001")
	if !ok {
		t.Fatal("GetTask 应能找到 job-001")
	}
	if task.Status != "PENDING" {
		t.Errorf("GetTask Status 应为 PENDING, 实际为 %s", task.Status)
	}
}

func TestKVStore_TaskTransition_PENDING_to_RUNNING(t *testing.T) {
	kv := NewKVStore()

	// 创建 PENDING 任务
	kv.Apply(buildTaskTx("job-001", "PENDING", ""))

	// 转换为 RUNNING
	result, err := kv.Apply(buildTaskTx("job-001", "RUNNING", "开始处理"))
	if err != nil {
		t.Fatalf("PENDING→RUNNING 失败: %v", err)
	}

	var applyResult ApplyResult
	json.Unmarshal(result, &applyResult)
	if !applyResult.Success {
		t.Errorf("PENDING→RUNNING 应成功, 错误: %s", applyResult.Error)
	}

	task, _ := kv.GetTask("job-001")
	if task.Status != "RUNNING" {
		t.Errorf("状态应为 RUNNING, 实际为 %s", task.Status)
	}
	if task.Message != "开始处理" {
		t.Errorf("Message 应为 '开始处理', 实际为 '%s'", task.Message)
	}
}

func TestKVStore_TaskTransition_RUNNING_to_SUCCESS(t *testing.T) {
	kv := NewKVStore()

	kv.Apply(buildTaskTx("job-001", "PENDING", ""))
	kv.Apply(buildTaskTx("job-001", "RUNNING", ""))

	result, _ := kv.Apply(buildTaskTx("job-001", "SUCCESS", "处理完成"))

	var applyResult ApplyResult
	json.Unmarshal(result, &applyResult)
	if !applyResult.Success {
		t.Errorf("RUNNING→SUCCESS 应成功, 错误: %s", applyResult.Error)
	}

	task, _ := kv.GetTask("job-001")
	if task.Status != "SUCCESS" {
		t.Errorf("状态应为 SUCCESS, 实际为 %s", task.Status)
	}
	if task.CompletedAt == "" {
		t.Error("终态应设置 CompletedAt")
	}
}

func TestKVStore_TaskTransition_RUNNING_to_FAILED(t *testing.T) {
	kv := NewKVStore()

	kv.Apply(buildTaskTx("job-001", "PENDING", ""))
	kv.Apply(buildTaskTx("job-001", "RUNNING", ""))

	result, _ := kv.Apply(buildTaskTx("job-001", "FAILED", "出错了"))

	var applyResult ApplyResult
	json.Unmarshal(result, &applyResult)
	if !applyResult.Success {
		t.Errorf("RUNNING→FAILED 应成功, 错误: %s", applyResult.Error)
	}

	task, _ := kv.GetTask("job-001")
	if task.Status != "FAILED" {
		t.Errorf("状态应为 FAILED, 实际为 %s", task.Status)
	}
	if task.CompletedAt == "" {
		t.Error("终态应设置 CompletedAt")
	}
}

func TestKVStore_TaskTransition_RUNNING_to_CANCELLED(t *testing.T) {
	kv := NewKVStore()

	kv.Apply(buildTaskTx("job-001", "PENDING", ""))
	kv.Apply(buildTaskTx("job-001", "RUNNING", ""))

	result, _ := kv.Apply(buildTaskTx("job-001", "CANCELLED", "用户取消"))

	var applyResult ApplyResult
	json.Unmarshal(result, &applyResult)
	if !applyResult.Success {
		t.Errorf("RUNNING→CANCELLED 应成功, 错误: %s", applyResult.Error)
	}
}

func TestKVStore_TaskTransition_RUNNING_to_TIMEOUT(t *testing.T) {
	kv := NewKVStore()

	kv.Apply(buildTaskTx("job-001", "PENDING", ""))
	kv.Apply(buildTaskTx("job-001", "RUNNING", ""))

	result, _ := kv.Apply(buildTaskTx("job-001", "TIMEOUT", "超时"))

	var applyResult ApplyResult
	json.Unmarshal(result, &applyResult)
	if !applyResult.Success {
		t.Errorf("RUNNING→TIMEOUT 应成功, 错误: %s", applyResult.Error)
	}

	task, _ := kv.GetTask("job-001")
	if task.Status != "TIMEOUT" {
		t.Errorf("状态应为 TIMEOUT, 实际为 %s", task.Status)
	}
}

func TestKVStore_TaskTransition_PENDING_to_CANCELLED(t *testing.T) {
	kv := NewKVStore()

	kv.Apply(buildTaskTx("job-001", "PENDING", ""))

	result, _ := kv.Apply(buildTaskTx("job-001", "CANCELLED", "取消"))

	var applyResult ApplyResult
	json.Unmarshal(result, &applyResult)
	if !applyResult.Success {
		t.Errorf("PENDING→CANCELLED 应成功, 错误: %s", applyResult.Error)
	}
}

func TestKVStore_TaskTransition_InvalidFromPENDING(t *testing.T) {
	kv := NewKVStore()

	kv.Apply(buildTaskTx("job-001", "PENDING", ""))

	// PENDING 不能直接到 SUCCESS
	result, _ := kv.Apply(buildTaskTx("job-001", "SUCCESS", ""))

	var applyResult ApplyResult
	json.Unmarshal(result, &applyResult)
	if applyResult.Success {
		t.Error("PENDING→SUCCESS 应被拒绝")
	}
	if applyResult.Error == "" {
		t.Error("拒绝的交易应有错误信息")
	}

	// 状态应保持 PENDING
	task, _ := kv.GetTask("job-001")
	if task.Status != "PENDING" {
		t.Errorf("被拒绝后状态应保持 PENDING, 实际为 %s", task.Status)
	}
}

func TestKVStore_TaskTransition_InvalidFromTerminal(t *testing.T) {
	kv := NewKVStore()

	// 走到终态 SUCCESS
	kv.Apply(buildTaskTx("job-001", "PENDING", ""))
	kv.Apply(buildTaskTx("job-001", "RUNNING", ""))
	kv.Apply(buildTaskTx("job-001", "SUCCESS", "done"))

	// 尝试从终态转换
	result, _ := kv.Apply(buildTaskTx("job-001", "RUNNING", "retry"))

	var applyResult ApplyResult
	json.Unmarshal(result, &applyResult)
	if applyResult.Success {
		t.Error("从终态 SUCCESS 转换应被拒绝")
	}

	task, _ := kv.GetTask("job-001")
	if task.Status != "SUCCESS" {
		t.Errorf("终态不应被修改, 实际为 %s", task.Status)
	}
}

func TestKVStore_TaskTransition_AllTerminalStatesAreImmutable(t *testing.T) {
	terminals := []string{"SUCCESS", "FAILED", "CANCELLED", "TIMEOUT"}

	for _, terminal := range terminals {
		t.Run(terminal, func(t *testing.T) {
			kv := NewKVStore()

			// 直接创建终态任务
			kv.Apply(buildTaskTx("job-"+terminal, "PENDING", ""))
			if terminal != "CANCELLED" {
				kv.Apply(buildTaskTx("job-"+terminal, "RUNNING", ""))
			}
			kv.Apply(buildTaskTx("job-"+terminal, terminal, ""))

			// 尝试修改终态
			result, _ := kv.Apply(buildTaskTx("job-"+terminal, "PENDING", ""))

			var applyResult ApplyResult
			json.Unmarshal(result, &applyResult)
			if applyResult.Success {
				t.Errorf("从终态 %s 不应允许转换", terminal)
			}
		})
	}
}

func TestKVStore_TaskConcurrentTransition_OnlyOneWins(t *testing.T) {
	kv := NewKVStore()

	// 创建 PENDING 任务
	kv.Apply(buildTaskTx("job-001", "PENDING", ""))

	// 第一个转换: PENDING→RUNNING 成功
	result1, _ := kv.Apply(buildTaskTx("job-001", "RUNNING", "A wins"))
	var r1 ApplyResult
	json.Unmarshal(result1, &r1)
	if !r1.Success {
		t.Fatal("第一个 PENDING→RUNNING 应成功")
	}

	// 第二个"并发"转换: 也想 PENDING→RUNNING，但状态已经是 RUNNING
	result2, _ := kv.Apply(buildTaskTx("job-001", "RUNNING", "B loses"))
	var r2 ApplyResult
	json.Unmarshal(result2, &r2)

	// RUNNING→RUNNING 不在允许的转换中，应被拒绝
	if r2.Success {
		t.Error("重复的 RUNNING→RUNNING 转换应被拒绝")
	}

	task, _ := kv.GetTask("job-001")
	if task.Message != "A wins" {
		t.Errorf("Message 应为第一个成功的 'A wins', 实际为 '%s'", task.Message)
	}
}

func TestKVStore_GetTask_ReturnsDeepCopy(t *testing.T) {
	kv := NewKVStore()
	kv.Apply(buildTaskTx("job-001", "PENDING", "original"))

	task1, _ := kv.GetTask("job-001")
	task1.Message = "modified"

	task2, _ := kv.GetTask("job-001")
	if task2.Message != "original" {
		t.Error("GetTask 应返回深拷贝，修改不应影响原数据")
	}
}

func TestKVStore_GetTask_NotFound(t *testing.T) {
	kv := NewKVStore()

	_, ok := kv.GetTask("nonexistent")
	if ok {
		t.Error("查询不存在的任务应返回 false")
	}
}

func TestKVStore_GetAllTasks(t *testing.T) {
	kv := NewKVStore()

	kv.Apply(buildTaskTx("job-001", "PENDING", ""))
	kv.Apply(buildTaskTx("job-002", "PENDING", ""))
	kv.Apply(buildTaskTx("job-003", "PENDING", ""))

	tasks := kv.GetAllTasks()
	if len(tasks) != 3 {
		t.Errorf("GetAllTasks 应返回 3 个任务, 实际为 %d", len(tasks))
	}
}

func TestKVStore_GetAllTasks_Empty(t *testing.T) {
	kv := NewKVStore()

	tasks := kv.GetAllTasks()
	if len(tasks) != 0 {
		t.Errorf("空状态 GetAllTasks 应返回 0 个任务, 实际为 %d", len(tasks))
	}
}

func TestKVStore_TaskInSnapshot(t *testing.T) {
	kv1 := NewKVStore()

	// 写入 KV 和 Task 数据
	cmd := raft.Command{Op: "SET", Key: "k1", Value: "v1"}
	data, _ := raft.EncodeCommand(cmd)
	kv1.Apply(data)

	kv1.Apply(buildTaskTx("job-001", "PENDING", ""))
	kv1.Apply(buildTaskTx("job-001", "RUNNING", "processing"))
	kv1.Apply(buildTaskTx("job-002", "PENDING", "another task"))

	// 快照
	snapshot, err := kv1.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot 失败: %v", err)
	}

	// 恢复到新 KVStore
	kv2 := NewKVStore()
	err = kv2.Restore(snapshot)
	if err != nil {
		t.Fatalf("Restore 失败: %v", err)
	}

	// 验证 KV 数据
	val, ok := kv2.Get("k1")
	if !ok || val != "v1" {
		t.Error("恢复后 KV 数据应一致")
	}

	// 验证 Task 数据
	task1, ok := kv2.GetTask("job-001")
	if !ok {
		t.Fatal("恢复后应能找到 job-001")
	}
	if task1.Status != "RUNNING" {
		t.Errorf("恢复后 job-001 状态应为 RUNNING, 实际为 %s", task1.Status)
	}
	if task1.Message != "processing" {
		t.Errorf("恢复后 job-001 消息应为 'processing', 实际为 '%s'", task1.Message)
	}

	task2, ok := kv2.GetTask("job-002")
	if !ok {
		t.Fatal("恢复后应能找到 job-002")
	}
	if task2.Status != "PENDING" {
		t.Errorf("恢复后 job-002 状态应为 PENDING, 实际为 %s", task2.Status)
	}

	tasks := kv2.GetAllTasks()
	if len(tasks) != 2 {
		t.Errorf("恢复后 GetAllTasks 应返回 2 个任务, 实际为 %d", len(tasks))
	}
}

func TestKVStore_UnknownTxType(t *testing.T) {
	kv := NewKVStore()

	tx := raft.Transaction{
		TxType:  "UNKNOWN_TYPE",
		Payload: []byte(`{}`),
	}
	data, _ := raft.EncodeTx(tx)

	_, err := kv.Apply(data)
	if err == nil {
		t.Error("未知交易类型应返回错误")
	}
}

func TestKVStore_InvalidTaskPayload(t *testing.T) {
	kv := NewKVStore()

	tx := raft.Transaction{
		TxType:  raft.TxTypeTask,
		Payload: []byte(`invalid json`),
	}
	data, _ := raft.EncodeTx(tx)

	_, err := kv.Apply(data)
	if err == nil {
		t.Error("无效 Task 载荷应返回错误")
	}
}

func TestKVStore_InvalidKVPayload(t *testing.T) {
	kv := NewKVStore()

	tx := raft.Transaction{
		TxType:  raft.TxTypeKV,
		Payload: []byte(`invalid json`),
	}
	data, _ := raft.EncodeTx(tx)

	_, err := kv.Apply(data)
	if err == nil {
		t.Error("无效 KV 载荷应返回错误")
	}
}

func TestValidateTransition(t *testing.T) {
	tests := []struct {
		from    string
		to      string
		wantErr bool
	}{
		// 合法转换
		{"PENDING", "RUNNING", false},
		{"PENDING", "CANCELLED", false},
		{"RUNNING", "SUCCESS", false},
		{"RUNNING", "FAILED", false},
		{"RUNNING", "CANCELLED", false},
		{"RUNNING", "TIMEOUT", false},
		// 非法转换
		{"PENDING", "SUCCESS", true},
		{"PENDING", "FAILED", true},
		{"PENDING", "TIMEOUT", true},
		{"RUNNING", "PENDING", true},
		{"RUNNING", "RUNNING", true},
		// 从终态转换（全部非法）
		{"SUCCESS", "RUNNING", true},
		{"SUCCESS", "PENDING", true},
		{"FAILED", "RUNNING", true},
		{"CANCELLED", "RUNNING", true},
		{"TIMEOUT", "RUNNING", true},
	}

	for _, tt := range tests {
		t.Run(tt.from+"→"+tt.to, func(t *testing.T) {
			err := validateTransition(tt.from, tt.to)
			if tt.wantErr && err == nil {
				t.Errorf("%s→%s 应返回错误", tt.from, tt.to)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("%s→%s 不应返回错误, 实际: %v", tt.from, tt.to, err)
			}
		})
	}
}

func TestIsTerminalStatus(t *testing.T) {
	terminals := []string{"SUCCESS", "FAILED", "CANCELLED", "TIMEOUT"}
	nonTerminals := []string{"PENDING", "RUNNING", "UNKNOWN", ""}

	for _, s := range terminals {
		if !isTerminalStatus(s) {
			t.Errorf("%s 应为终态", s)
		}
	}
	for _, s := range nonTerminals {
		if isTerminalStatus(s) {
			t.Errorf("%s 不应为终态", s)
		}
	}
}

func TestKVStore_GetAll_IncludesTasks(t *testing.T) {
	kv := NewKVStore()

	// 写入 KV
	cmd := raft.Command{Op: "SET", Key: "name", Value: "Alice"}
	data, _ := raft.EncodeCommand(cmd)
	kv.Apply(data)

	// 写入 Task
	kv.Apply(buildTaskTx("job-001", "PENDING", "test"))

	all := kv.GetAll()
	if _, ok := all["name"]; !ok {
		t.Error("GetAll 应包含 KV 数据")
	}
	if _, ok := all["task:job-001"]; !ok {
		t.Error("GetAll 应包含以 task: 为前缀的任务数据")
	}
}
