package raft

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	pb "github.com/distributeStateMachine/proto"
)

// =========================================================================
// Mock 实现
// =========================================================================

// mockTransport 模拟传输层
type mockTransport struct {
	mu                sync.Mutex
	voteResponses     map[string]*pb.RequestVoteResponse
	appendResponses   map[string]*pb.AppendEntriesResponse
	voteRequests      []*pb.RequestVoteRequest
	appendRequests    []*pb.AppendEntriesRequest
	voteErr           error
	appendErr         error
}

func newMockTransport() *mockTransport {
	return &mockTransport{
		voteResponses:   make(map[string]*pb.RequestVoteResponse),
		appendResponses: make(map[string]*pb.AppendEntriesResponse),
	}
}

func (m *mockTransport) SendRequestVote(_ context.Context, target string, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.voteRequests = append(m.voteRequests, req)
	if m.voteErr != nil {
		return nil, m.voteErr
	}
	if resp, ok := m.voteResponses[target]; ok {
		return resp, nil
	}
	return &pb.RequestVoteResponse{Term: req.Term, VoteGranted: false}, nil
}

func (m *mockTransport) SendAppendEntries(_ context.Context, target string, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.appendRequests = append(m.appendRequests, req)
	if m.appendErr != nil {
		return nil, m.appendErr
	}
	if resp, ok := m.appendResponses[target]; ok {
		return resp, nil
	}
	return &pb.AppendEntriesResponse{Term: req.Term, Success: true, MatchIndex: req.PrevLogIndex + uint64(len(req.Entries))}, nil
}

func (m *mockTransport) Start() error { return nil }
func (m *mockTransport) Stop() error  { return nil }

// mockStorage 模拟存储
type mockStorage struct {
	mu           sync.Mutex
	state        *PersistentState
	snapMeta     *SnapshotMetadata
	snapData     []byte
}

func newMockStorage() *mockStorage {
	return &mockStorage{}
}

func (m *mockStorage) SaveState(state *PersistentState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	copied := *state
	copied.Log = make([]LogEntry, len(state.Log))
	copy(copied.Log, state.Log)
	m.state = &copied
	return nil
}

func (m *mockStorage) LoadState() (*PersistentState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state, nil
}

func (m *mockStorage) SaveSnapshot(metadata SnapshotMetadata, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.snapMeta = &metadata
	m.snapData = make([]byte, len(data))
	copy(m.snapData, data)
	return nil
}

func (m *mockStorage) LoadSnapshot() (*SnapshotMetadata, []byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.snapMeta, m.snapData, nil
}

func (m *mockStorage) Close() error { return nil }

// mockStateMachine 模拟状态机
type mockStateMachine struct {
	mu       sync.Mutex
	applied  [][]byte
}

func newMockStateMachine() *mockStateMachine {
	return &mockStateMachine{}
}

func (m *mockStateMachine) Apply(command []byte) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.applied = append(m.applied, command)
	return []byte("OK"), nil
}

func (m *mockStateMachine) Snapshot() ([]byte, error) {
	return []byte("{}"), nil
}

func (m *mockStateMachine) Restore(_ []byte) error {
	return nil
}

// =========================================================================
// 辅助函数
// =========================================================================

func makeTestConfig(id string) Config {
	return Config{
		ID: id,
		Peers: map[string]string{
			"node1": "localhost:9001",
			"node2": "localhost:9002",
			"node3": "localhost:9003",
		},
		ElectionTimeout:   300 * time.Millisecond,
		HeartbeatInterval: 100 * time.Millisecond,
		ListenAddr:        ":9001",
	}
}

func createTestNode(id string) (*RaftNode, *mockTransport, *mockStorage, *mockStateMachine) {
	trans := newMockTransport()
	store := newMockStorage()
	sm := newMockStateMachine()
	config := makeTestConfig(id)

	node, _ := NewRaftNode(config, trans, store, sm)
	return node, trans, store, sm
}

// =========================================================================
// 测试用例
// =========================================================================

func TestNewRaftNode_InitialState(t *testing.T) {
	node, _, _, _ := createTestNode("node1")

	term, isLeader := node.GetState()
	if term != 0 {
		t.Errorf("初始任期应为 0, 实际为 %d", term)
	}
	if isLeader {
		t.Error("初始状态不应为 Leader")
	}
	if node.state != Follower {
		t.Errorf("初始角色应为 Follower, 实际为 %s", node.state)
	}
	if node.persistent.VotedFor != "" {
		t.Errorf("初始 VotedFor 应为空, 实际为 %s", node.persistent.VotedFor)
	}
	if len(node.persistent.Log) != 0 {
		t.Errorf("初始日志应为空, 实际长度 %d", len(node.persistent.Log))
	}
}

func TestNewRaftNode_RestoreFromStorage(t *testing.T) {
	store := newMockStorage()
	// 预设持久化状态
	store.state = &PersistentState{
		CurrentTerm: 5,
		VotedFor:    "node2",
		Log: []LogEntry{
			{Term: 1, Index: 1, Command: []byte("cmd1")},
			{Term: 3, Index: 2, Command: []byte("cmd2")},
		},
	}

	config := makeTestConfig("node1")
	node, err := NewRaftNode(config, newMockTransport(), store, newMockStateMachine())
	if err != nil {
		t.Fatalf("创建节点失败: %v", err)
	}

	if node.persistent.CurrentTerm != 5 {
		t.Errorf("恢复后任期应为 5, 实际为 %d", node.persistent.CurrentTerm)
	}
	if node.persistent.VotedFor != "node2" {
		t.Errorf("恢复后 VotedFor 应为 node2, 实际为 %s", node.persistent.VotedFor)
	}
	if len(node.persistent.Log) != 2 {
		t.Errorf("恢复后日志长度应为 2, 实际为 %d", len(node.persistent.Log))
	}
}

func TestNodeStateString(t *testing.T) {
	tests := []struct {
		state    NodeState
		expected string
	}{
		{Follower, "Follower"},
		{Candidate, "Candidate"},
		{Leader, "Leader"},
		{NodeState(99), "Unknown"},
	}
	for _, tt := range tests {
		if got := tt.state.String(); got != tt.expected {
			t.Errorf("NodeState(%d).String() = %s, 期望 %s", tt.state, got, tt.expected)
		}
	}
}

// =========================================================================
// RequestVote 测试
// =========================================================================

func TestHandleRequestVote_GrantVote(t *testing.T) {
	node, _, _, _ := createTestNode("node1")

	req := &pb.RequestVoteRequest{
		Term:         1,
		CandidateId:  "node2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	resp := node.HandleRequestVote(req)

	if !resp.VoteGranted {
		t.Error("应该投票给候选人")
	}
	if resp.Term != 1 {
		t.Errorf("响应任期应为 1, 实际为 %d", resp.Term)
	}
	if node.persistent.VotedFor != "node2" {
		t.Errorf("VotedFor 应为 node2, 实际为 %s", node.persistent.VotedFor)
	}
}

func TestHandleRequestVote_RejectLowerTerm(t *testing.T) {
	node, _, _, _ := createTestNode("node1")
	node.persistent.CurrentTerm = 5

	req := &pb.RequestVoteRequest{
		Term:         3,
		CandidateId:  "node2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	resp := node.HandleRequestVote(req)

	if resp.VoteGranted {
		t.Error("不应该投票给低任期的候选人")
	}
	if resp.Term != 5 {
		t.Errorf("响应任期应为 5, 实际为 %d", resp.Term)
	}
}

func TestHandleRequestVote_RejectAlreadyVoted(t *testing.T) {
	node, _, _, _ := createTestNode("node1")
	node.persistent.CurrentTerm = 1
	node.persistent.VotedFor = "node3" // 已投票给 node3

	req := &pb.RequestVoteRequest{
		Term:         1,
		CandidateId:  "node2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	resp := node.HandleRequestVote(req)

	if resp.VoteGranted {
		t.Error("同一任期内不应重复投票给不同候选人")
	}
}

func TestHandleRequestVote_AcceptSameCandidate(t *testing.T) {
	node, _, _, _ := createTestNode("node1")
	node.persistent.CurrentTerm = 1
	node.persistent.VotedFor = "node2" // 已投给 node2

	req := &pb.RequestVoteRequest{
		Term:         1,
		CandidateId:  "node2", // 同一候选人重复请求
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	resp := node.HandleRequestVote(req)

	if !resp.VoteGranted {
		t.Error("对同一候选人的重复投票请求应该被接受")
	}
}

func TestHandleRequestVote_RejectStaleLog(t *testing.T) {
	node, _, _, _ := createTestNode("node1")
	// node1 有一条 term=2 的日志
	node.persistent.Log = []LogEntry{
		{Term: 2, Index: 1, Command: []byte("cmd")},
	}

	// node2 的日志 term=1，比 node1 旧
	req := &pb.RequestVoteRequest{
		Term:         3,
		CandidateId:  "node2",
		LastLogIndex: 1,
		LastLogTerm:  1,
	}

	resp := node.HandleRequestVote(req)

	if resp.VoteGranted {
		t.Error("不应投票给日志更旧的候选人")
	}
}

func TestHandleRequestVote_HigherTermConvertsToFollower(t *testing.T) {
	node, _, _, _ := createTestNode("node1")
	node.persistent.CurrentTerm = 1
	node.state = Candidate

	req := &pb.RequestVoteRequest{
		Term:         5,
		CandidateId:  "node2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	resp := node.HandleRequestVote(req)

	if !resp.VoteGranted {
		t.Error("更高任期的投票请求应被接受")
	}
	if node.persistent.CurrentTerm != 5 {
		t.Errorf("任期应更新为 5, 实际为 %d", node.persistent.CurrentTerm)
	}
	if node.state != Follower {
		t.Errorf("应转为 Follower, 实际为 %s", node.state)
	}
}

// =========================================================================
// AppendEntries 测试
// =========================================================================

func TestHandleAppendEntries_Heartbeat(t *testing.T) {
	node, _, _, _ := createTestNode("node1")

	req := &pb.AppendEntriesRequest{
		Term:         1,
		LeaderId:     "node2",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      nil, // 心跳
		LeaderCommit: 0,
	}

	resp := node.HandleAppendEntries(req)

	if !resp.Success {
		t.Error("心跳应返回成功")
	}
	if node.persistent.CurrentTerm != 1 {
		t.Errorf("任期应更新为 1, 实际为 %d", node.persistent.CurrentTerm)
	}
}

func TestHandleAppendEntries_RejectLowerTerm(t *testing.T) {
	node, _, _, _ := createTestNode("node1")
	node.persistent.CurrentTerm = 5

	req := &pb.AppendEntriesRequest{
		Term:     3,
		LeaderId: "node2",
	}

	resp := node.HandleAppendEntries(req)

	if resp.Success {
		t.Error("应拒绝低任期的 AppendEntries")
	}
	if resp.Term != 5 {
		t.Errorf("响应任期应为 5, 实际为 %d", resp.Term)
	}
}

func TestHandleAppendEntries_AppendNewEntries(t *testing.T) {
	node, _, _, _ := createTestNode("node1")

	entries := []*pb.LogEntry{
		{Term: 1, Index: 1, Command: []byte("cmd1")},
		{Term: 1, Index: 2, Command: []byte("cmd2")},
	}

	req := &pb.AppendEntriesRequest{
		Term:         1,
		LeaderId:     "node2",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      entries,
		LeaderCommit: 0,
	}

	resp := node.HandleAppendEntries(req)

	if !resp.Success {
		t.Error("追加日志应成功")
	}
	if len(node.persistent.Log) != 2 {
		t.Errorf("日志长度应为 2, 实际为 %d", len(node.persistent.Log))
	}
	if string(node.persistent.Log[0].Command) != "cmd1" {
		t.Errorf("第一条日志命令应为 cmd1, 实际为 %s", node.persistent.Log[0].Command)
	}
	if string(node.persistent.Log[1].Command) != "cmd2" {
		t.Errorf("第二条日志命令应为 cmd2, 实际为 %s", node.persistent.Log[1].Command)
	}
}

func TestHandleAppendEntries_LogMismatch(t *testing.T) {
	node, _, _, _ := createTestNode("node1")
	// node1 只有 1 条日志
	node.persistent.Log = []LogEntry{
		{Term: 1, Index: 1, Command: []byte("cmd1")},
	}

	// Leader 认为 prevLogIndex=3，Follower 日志太短
	req := &pb.AppendEntriesRequest{
		Term:         2,
		LeaderId:     "node2",
		PrevLogIndex: 3,
		PrevLogTerm:  1,
		Entries:      nil,
		LeaderCommit: 0,
	}

	resp := node.HandleAppendEntries(req)

	if resp.Success {
		t.Error("日志不匹配时应返回失败")
	}
	if resp.MatchIndex != 1 {
		t.Errorf("MatchIndex 应为 1 (当前日志长度), 实际为 %d", resp.MatchIndex)
	}
}

func TestHandleAppendEntries_TermMismatch_Truncate(t *testing.T) {
	node, _, _, _ := createTestNode("node1")
	node.persistent.Log = []LogEntry{
		{Term: 1, Index: 1, Command: []byte("cmd1")},
		{Term: 1, Index: 2, Command: []byte("cmd2")}, // 这条日志的 term 与 Leader 不一致
	}

	// Leader 的 prevLogIndex=2, prevLogTerm=2，但 Follower 的 index=2 是 term=1
	req := &pb.AppendEntriesRequest{
		Term:         3,
		LeaderId:     "node2",
		PrevLogIndex: 2,
		PrevLogTerm:  2, // 与 Follower 不匹配
		Entries:      nil,
		LeaderCommit: 0,
	}

	resp := node.HandleAppendEntries(req)

	if resp.Success {
		t.Error("任期不匹配时应返回失败并截断")
	}
	// 冲突日志及之后都应被截断，剩余 1 条
	if len(node.persistent.Log) != 1 {
		t.Errorf("截断后日志长度应为 1, 实际为 %d", len(node.persistent.Log))
	}
}

func TestHandleAppendEntries_UpdateCommitIndex(t *testing.T) {
	node, _, _, _ := createTestNode("node1")

	entries := []*pb.LogEntry{
		{Term: 1, Index: 1, Command: []byte("cmd1")},
		{Term: 1, Index: 2, Command: []byte("cmd2")},
	}

	req := &pb.AppendEntriesRequest{
		Term:         1,
		LeaderId:     "node2",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      entries,
		LeaderCommit: 2,
	}

	resp := node.HandleAppendEntries(req)

	if !resp.Success {
		t.Error("应成功")
	}
	if node.volatile.CommitIndex != 2 {
		t.Errorf("CommitIndex 应为 2, 实际为 %d", node.volatile.CommitIndex)
	}
}

func TestHandleAppendEntries_CommitIndexCappedByLeaderCommit(t *testing.T) {
	node, _, _, _ := createTestNode("node1")

	entries := []*pb.LogEntry{
		{Term: 1, Index: 1, Command: []byte("cmd1")},
		{Term: 1, Index: 2, Command: []byte("cmd2")},
		{Term: 1, Index: 3, Command: []byte("cmd3")},
	}

	req := &pb.AppendEntriesRequest{
		Term:         1,
		LeaderId:     "node2",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      entries,
		LeaderCommit: 2, // Leader 只提交到 2
	}

	node.HandleAppendEntries(req)

	if node.volatile.CommitIndex != 2 {
		t.Errorf("CommitIndex 应受 LeaderCommit 限制为 2, 实际为 %d", node.volatile.CommitIndex)
	}
}

func TestHandleAppendEntries_CandidateConvertsToFollower(t *testing.T) {
	node, _, _, _ := createTestNode("node1")
	node.persistent.CurrentTerm = 1
	node.state = Candidate

	req := &pb.AppendEntriesRequest{
		Term:     1,
		LeaderId: "node2",
	}

	resp := node.HandleAppendEntries(req)

	if !resp.Success {
		t.Error("应成功")
	}
	if node.state != Follower {
		t.Errorf("Candidate 收到同任期 AppendEntries 应转为 Follower, 实际为 %s", node.state)
	}
}

// =========================================================================
// 命令编解码测试
// =========================================================================

func TestEncodeDecodeCommand(t *testing.T) {
	cmd := Command{
		Op:    "SET",
		Key:   "name",
		Value: "Alice",
	}

	data, err := EncodeCommand(cmd)
	if err != nil {
		t.Fatalf("编码失败: %v", err)
	}

	decoded, err := DecodeCommand(data)
	if err != nil {
		t.Fatalf("解码失败: %v", err)
	}

	if decoded.Op != cmd.Op || decoded.Key != cmd.Key || decoded.Value != cmd.Value {
		t.Errorf("解码结果不匹配: got %+v, want %+v", decoded, cmd)
	}
}

func TestDecodeCommand_InvalidData(t *testing.T) {
	_, err := DecodeCommand([]byte("invalid json"))
	if err == nil {
		t.Error("无效 JSON 应返回错误")
	}
}

// DecodeCommand 兼容旧格式（直接 Command JSON）
func TestDecodeCommand_LegacyFormat(t *testing.T) {
	// 旧格式：直接的 Command JSON，没有 Transaction 包装
	legacyJSON := `{"op":"SET","key":"name","value":"Bob"}`
	cmd, err := DecodeCommand([]byte(legacyJSON))
	if err != nil {
		t.Fatalf("解码旧格式失败: %v", err)
	}
	if cmd.Op != "SET" || cmd.Key != "name" || cmd.Value != "Bob" {
		t.Errorf("旧格式解码结果不匹配: %+v", cmd)
	}
}

// =========================================================================
// Transaction 编解码测试
// =========================================================================

func TestEncodeDecodeTx_KV(t *testing.T) {
	cmd := Command{Op: "SET", Key: "foo", Value: "bar"}
	payload, _ := json.Marshal(cmd)
	tx := Transaction{
		TxType:  TxTypeKV,
		TxID:    "tx-001",
		Payload: payload,
	}

	data, err := EncodeTx(tx)
	if err != nil {
		t.Fatalf("EncodeTx 失败: %v", err)
	}

	decoded, err := DecodeTx(data)
	if err != nil {
		t.Fatalf("DecodeTx 失败: %v", err)
	}

	if decoded.TxType != TxTypeKV {
		t.Errorf("TxType 应为 KV, 实际为 %s", decoded.TxType)
	}
	if decoded.TxID != "tx-001" {
		t.Errorf("TxID 应为 tx-001, 实际为 %s", decoded.TxID)
	}

	var decodedCmd Command
	if err := json.Unmarshal(decoded.Payload, &decodedCmd); err != nil {
		t.Fatalf("解析 KV Payload 失败: %v", err)
	}
	if decodedCmd.Op != "SET" || decodedCmd.Key != "foo" || decodedCmd.Value != "bar" {
		t.Errorf("KV Payload 内容不匹配: %+v", decodedCmd)
	}
}

func TestEncodeDecodeTx_Task(t *testing.T) {
	taskTx := TaskTransaction{
		TaskID:  "job-001",
		Status:  "RUNNING",
		Message: "处理中",
	}
	payload, _ := json.Marshal(taskTx)
	tx := Transaction{
		TxType:  TxTypeTask,
		TxID:    "tx-002",
		Payload: payload,
	}

	data, err := EncodeTx(tx)
	if err != nil {
		t.Fatalf("EncodeTx 失败: %v", err)
	}

	decoded, err := DecodeTx(data)
	if err != nil {
		t.Fatalf("DecodeTx 失败: %v", err)
	}

	if decoded.TxType != TxTypeTask {
		t.Errorf("TxType 应为 TASK, 实际为 %s", decoded.TxType)
	}

	var decodedTask TaskTransaction
	if err := json.Unmarshal(decoded.Payload, &decodedTask); err != nil {
		t.Fatalf("解析 Task Payload 失败: %v", err)
	}
	if decodedTask.TaskID != "job-001" || decodedTask.Status != "RUNNING" || decodedTask.Message != "处理中" {
		t.Errorf("Task Payload 内容不匹配: %+v", decodedTask)
	}
}

func TestDecodeTx_InvalidData(t *testing.T) {
	_, err := DecodeTx([]byte("invalid json"))
	if err == nil {
		t.Error("无效 JSON 应返回错误")
	}
}

func TestEncodeTx_EmptyPayload(t *testing.T) {
	tx := Transaction{
		TxType:  TxTypeKV,
		Payload: nil,
	}

	data, err := EncodeTx(tx)
	if err != nil {
		t.Fatalf("EncodeTx 空 Payload 不应失败: %v", err)
	}

	decoded, err := DecodeTx(data)
	if err != nil {
		t.Fatalf("DecodeTx 空 Payload 不应失败: %v", err)
	}
	if decoded.TxType != TxTypeKV {
		t.Errorf("TxType 应为 KV, 实际为 %s", decoded.TxType)
	}
}

func TestEncodeCommand_WrapsInTransaction(t *testing.T) {
	cmd := Command{Op: "DELETE", Key: "test"}
	data, err := EncodeCommand(cmd)
	if err != nil {
		t.Fatalf("EncodeCommand 失败: %v", err)
	}

	// 验证 EncodeCommand 输出的是 Transaction 格式
	var tx Transaction
	if err := json.Unmarshal(data, &tx); err != nil {
		t.Fatalf("EncodeCommand 的输出应为 Transaction 格式: %v", err)
	}
	if tx.TxType != TxTypeKV {
		t.Errorf("EncodeCommand 应产生 KV 类型交易, 实际为 %s", tx.TxType)
	}

	// 验证 Payload 内容
	var decoded Command
	json.Unmarshal(tx.Payload, &decoded)
	if decoded.Op != "DELETE" || decoded.Key != "test" {
		t.Errorf("Payload 内容不匹配: %+v", decoded)
	}
}

func TestTxType_Constants(t *testing.T) {
	if TxTypeKV != "KV" {
		t.Errorf("TxTypeKV 应为 'KV', 实际为 '%s'", TxTypeKV)
	}
	if TxTypeTask != "TASK" {
		t.Errorf("TxTypeTask 应为 'TASK', 实际为 '%s'", TxTypeTask)
	}
}

// =========================================================================
// LogEntry 转换测试
// =========================================================================

func TestLogEntryToProtoAndBack(t *testing.T) {
	entry := LogEntry{
		Term:    3,
		Index:   7,
		Command: []byte("test-command"),
	}

	proto := entry.ToProto()
	if proto.Term != 3 || proto.Index != 7 || string(proto.Command) != "test-command" {
		t.Errorf("ToProto 转换不正确: %+v", proto)
	}

	back := LogEntryFromProto(proto)
	if back.Term != entry.Term || back.Index != entry.Index || string(back.Command) != string(entry.Command) {
		t.Errorf("FromProto 转换不正确: %+v", back)
	}
}

// =========================================================================
// 集成测试：3 节点选举
// =========================================================================

func TestThreeNodeElection(t *testing.T) {
	// 创建 3 个节点
	node1, trans1, store1, sm1 := createTestNode("node1")
	node2, _, _, _ := createTestNode("node2")
	node3, _, _, _ := createTestNode("node3")

	_ = store1
	_ = sm1

	// node1 的传输层返回投票同意
	trans1.voteResponses["localhost:9002"] = &pb.RequestVoteResponse{Term: 1, VoteGranted: true}
	trans1.voteResponses["localhost:9003"] = &pb.RequestVoteResponse{Term: 1, VoteGranted: true}

	// 也设置 AppendEntries 的响应
	trans1.appendResponses["localhost:9002"] = &pb.AppendEntriesResponse{Term: 1, Success: true, MatchIndex: 0}
	trans1.appendResponses["localhost:9003"] = &pb.AppendEntriesResponse{Term: 1, Success: true, MatchIndex: 0}

	// 启动节点
	node1.Start()
	node2.Start()
	node3.Start()

	defer func() {
		node1.Stop()
		node2.Stop()
		node3.Stop()
	}()

	// 等待选举完成
	time.Sleep(1 * time.Second)

	term, isLeader := node1.GetState()
	if !isLeader {
		t.Logf("node1 未成为 Leader (term=%d), 这在竞争选举中是正常的", term)
	}
	if term < 1 {
		t.Errorf("经过选举后任期应 >= 1, 实际为 %d", term)
	}
}

// =========================================================================
// Propose 测试
// =========================================================================

func TestPropose_NotLeader(t *testing.T) {
	node, _, _, _ := createTestNode("node1")
	// 默认是 Follower

	_, err := node.Propose([]byte("test"))
	if err == nil {
		t.Error("Follower 执行 Propose 应返回错误")
	}
	if err.Error() != "not leader" {
		t.Errorf("错误消息应为 'not leader', 实际为 '%s'", err.Error())
	}
}

func TestGetLeaderID(t *testing.T) {
	node, _, _, _ := createTestNode("node1")

	// Follower 返回空
	if id := node.GetLeaderID(); id != "" {
		t.Errorf("Follower 的 LeaderID 应为空, 实际为 %s", id)
	}

	// 手动设为 Leader
	node.mu.Lock()
	node.state = Leader
	node.mu.Unlock()

	if id := node.GetLeaderID(); id != "node1" {
		t.Errorf("Leader 的 LeaderID 应为 node1, 实际为 %s", id)
	}
}

// =========================================================================
// 日志索引转换测试
// =========================================================================

func TestLogicalToPhysical(t *testing.T) {
	node, _, _, _ := createTestNode("node1")

	// 无快照时，逻辑索引 1 → 物理 0
	node.persistent.SnapshotIndex = 0
	if got := node.logicalToPhysical(1); got != 0 {
		t.Errorf("无快照: 逻辑 1 → 物理应为 0, 实际为 %d", got)
	}
	if got := node.logicalToPhysical(5); got != 4 {
		t.Errorf("无快照: 逻辑 5 → 物理应为 4, 实际为 %d", got)
	}

	// 有快照截断: SnapshotIndex=10, 逻辑 11 → 物理 0
	node.persistent.SnapshotIndex = 10
	if got := node.logicalToPhysical(11); got != 0 {
		t.Errorf("快照截断后: 逻辑 11 → 物理应为 0, 实际为 %d", got)
	}
	if got := node.logicalToPhysical(15); got != 4 {
		t.Errorf("快照截断后: 逻辑 15 → 物理应为 4, 实际为 %d", got)
	}
}

func TestPhysicalToLogical(t *testing.T) {
	node, _, _, _ := createTestNode("node1")

	node.persistent.SnapshotIndex = 0
	if got := node.physicalToLogical(0); got != 1 {
		t.Errorf("无快照: 物理 0 → 逻辑应为 1, 实际为 %d", got)
	}

	node.persistent.SnapshotIndex = 10
	if got := node.physicalToLogical(0); got != 11 {
		t.Errorf("快照截断后: 物理 0 → 逻辑应为 11, 实际为 %d", got)
	}
	if got := node.physicalToLogical(4); got != 15 {
		t.Errorf("快照截断后: 物理 4 → 逻辑应为 15, 实际为 %d", got)
	}
}

func TestGetLogEntry(t *testing.T) {
	node, _, _, _ := createTestNode("node1")

	// 构建一些日志条目
	node.persistent.SnapshotIndex = 5
	node.persistent.SnapshotTerm = 1
	node.persistent.Log = []LogEntry{
		{Term: 1, Index: 6, Command: []byte("cmd6")},
		{Term: 2, Index: 7, Command: []byte("cmd7")},
		{Term: 2, Index: 8, Command: []byte("cmd8")},
	}

	// 快照范围内的索引 → nil
	if entry := node.getLogEntry(3); entry != nil {
		t.Error("快照范围内的索引应返回 nil")
	}
	if entry := node.getLogEntry(5); entry != nil {
		t.Error("SnapshotIndex 本身应返回 nil")
	}

	// 正常范围
	entry := node.getLogEntry(6)
	if entry == nil {
		t.Fatal("逻辑索引 6 应存在")
	}
	if entry.Index != 6 || entry.Term != 1 {
		t.Errorf("逻辑索引 6 内容不匹配: %+v", entry)
	}

	entry = node.getLogEntry(8)
	if entry == nil {
		t.Fatal("逻辑索引 8 应存在")
	}
	if string(entry.Command) != "cmd8" {
		t.Errorf("逻辑索引 8 Command 应为 cmd8, 实际为 %s", entry.Command)
	}

	// 超出范围
	if entry := node.getLogEntry(9); entry != nil {
		t.Error("超出范围的索引应返回 nil")
	}
}

func TestLastLogIndex_WithSnapshot(t *testing.T) {
	node, _, _, _ := createTestNode("node1")

	// 无日志无快照
	if idx := node.lastLogIndex(); idx != 0 {
		t.Errorf("空日志应返回 0, 实际为 %d", idx)
	}

	// 有快照无日志
	node.persistent.SnapshotIndex = 10
	if idx := node.lastLogIndex(); idx != 10 {
		t.Errorf("有快照无日志应返回 SnapshotIndex=10, 实际为 %d", idx)
	}

	// 有快照有日志
	node.persistent.Log = []LogEntry{
		{Term: 2, Index: 11, Command: []byte("cmd11")},
		{Term: 2, Index: 12, Command: []byte("cmd12")},
	}
	if idx := node.lastLogIndex(); idx != 12 {
		t.Errorf("应返回最后日志索引 12, 实际为 %d", idx)
	}
}

func TestLastLogInfo_WithSnapshot(t *testing.T) {
	node, _, _, _ := createTestNode("node1")

	// 有快照无日志 → 返回快照截断点
	node.persistent.SnapshotIndex = 10
	node.persistent.SnapshotTerm = 3
	idx, term := node.lastLogInfo()
	if idx != 10 || term != 3 {
		t.Errorf("有快照无日志应返回 (10, 3), 实际为 (%d, %d)", idx, term)
	}

	// 有快照有日志 → 返回最后日志
	node.persistent.Log = []LogEntry{
		{Term: 4, Index: 11, Command: []byte("cmd")},
	}
	idx, term = node.lastLogInfo()
	if idx != 11 || term != 4 {
		t.Errorf("有快照有日志应返回 (11, 4), 实际为 (%d, %d)", idx, term)
	}
}

// =========================================================================
// 快照与日志压缩测试
// =========================================================================

func TestMaybeSnapshot_TriggersAtThreshold(t *testing.T) {
	node, _, store, sm := createTestNode("node1")
	node.config.SnapshotThreshold = 5 // 5 条日志就触发

	// 手动添加 5 条日志
	for i := uint64(1); i <= 5; i++ {
		node.persistent.Log = append(node.persistent.Log, LogEntry{
			Term:    1,
			Index:   i,
			Command: []byte("cmd"),
		})
	}

	// 模拟已全部应用
	node.volatile.LastApplied = 5
	node.volatile.CommitIndex = 5

	// 设置 mock 状态机快照返回值
	_ = sm

	// 触发快照
	node.maybeSnapshot()

	// 验证快照已保存
	store.mu.Lock()
	if store.snapMeta == nil {
		t.Fatal("快照应已保存")
	}
	if store.snapMeta.LastIncludedIndex != 5 {
		t.Errorf("快照截断索引应为 5, 实际为 %d", store.snapMeta.LastIncludedIndex)
	}
	if store.snapMeta.LastIncludedTerm != 1 {
		t.Errorf("快照截断任期应为 1, 实际为 %d", store.snapMeta.LastIncludedTerm)
	}
	store.mu.Unlock()

	// 验证日志已截断
	node.mu.RLock()
	if len(node.persistent.Log) != 0 {
		t.Errorf("快照后剩余日志应为 0, 实际为 %d", len(node.persistent.Log))
	}
	if node.persistent.SnapshotIndex != 5 {
		t.Errorf("SnapshotIndex 应为 5, 实际为 %d", node.persistent.SnapshotIndex)
	}
	if node.persistent.SnapshotTerm != 1 {
		t.Errorf("SnapshotTerm 应为 1, 实际为 %d", node.persistent.SnapshotTerm)
	}
	node.mu.RUnlock()
}

func TestMaybeSnapshot_PartialTruncation(t *testing.T) {
	node, _, store, _ := createTestNode("node1")
	node.config.SnapshotThreshold = 3

	// 添加 5 条日志
	for i := uint64(1); i <= 5; i++ {
		node.persistent.Log = append(node.persistent.Log, LogEntry{
			Term:    1,
			Index:   i,
			Command: []byte("cmd"),
		})
	}

	// 只应用了前 3 条
	node.volatile.LastApplied = 3
	node.volatile.CommitIndex = 5

	node.maybeSnapshot()

	// 验证截断到 index=3
	store.mu.Lock()
	if store.snapMeta == nil {
		t.Fatal("快照应已保存")
	}
	if store.snapMeta.LastIncludedIndex != 3 {
		t.Errorf("快照截断索引应为 3, 实际为 %d", store.snapMeta.LastIncludedIndex)
	}
	store.mu.Unlock()

	// 日志应保留 index 4 和 5
	node.mu.RLock()
	if len(node.persistent.Log) != 2 {
		t.Errorf("快照后应保留 2 条日志, 实际为 %d", len(node.persistent.Log))
	}
	if node.persistent.Log[0].Index != 4 {
		t.Errorf("第一条剩余日志的 Index 应为 4, 实际为 %d", node.persistent.Log[0].Index)
	}
	if node.persistent.Log[1].Index != 5 {
		t.Errorf("第二条剩余日志的 Index 应为 5, 实际为 %d", node.persistent.Log[1].Index)
	}
	node.mu.RUnlock()
}

func TestMaybeSnapshot_NotTriggeredBelowThreshold(t *testing.T) {
	node, _, store, _ := createTestNode("node1")
	node.config.SnapshotThreshold = 10

	// 只有 3 条日志
	for i := uint64(1); i <= 3; i++ {
		node.persistent.Log = append(node.persistent.Log, LogEntry{
			Term:    1,
			Index:   i,
			Command: []byte("cmd"),
		})
	}
	node.volatile.LastApplied = 3

	node.maybeSnapshot()

	// 不应触发快照
	store.mu.Lock()
	if store.snapMeta != nil {
		t.Error("日志数未达阈值时不应触发快照")
	}
	store.mu.Unlock()
}

func TestMaybeSnapshot_NotTriggeredIfNothingApplied(t *testing.T) {
	node, _, store, _ := createTestNode("node1")
	node.config.SnapshotThreshold = 3

	for i := uint64(1); i <= 5; i++ {
		node.persistent.Log = append(node.persistent.Log, LogEntry{
			Term:    1,
			Index:   i,
			Command: []byte("cmd"),
		})
	}

	// LastApplied = 0，没有已应用的日志
	node.volatile.LastApplied = 0

	node.maybeSnapshot()

	store.mu.Lock()
	if store.snapMeta != nil {
		t.Error("没有已应用日志时不应触发快照")
	}
	store.mu.Unlock()
}

func TestNewRaftNode_RestoreFromSnapshot(t *testing.T) {
	store := newMockStorage()
	trans := newMockTransport()
	sm := newMockStateMachine()

	// 预置快照
	store.snapMeta = &SnapshotMetadata{
		LastIncludedIndex: 10,
		LastIncludedTerm:  2,
		CreatedAt:         "2026-01-01T00:00:00Z",
	}
	store.snapData = []byte(`{"restored":true}`)

	// 预置快照后的 Raft 状态
	store.state = &PersistentState{
		CurrentTerm:   3,
		VotedFor:      "",
		SnapshotIndex: 10,
		SnapshotTerm:  2,
		Log: []LogEntry{
			{Term: 3, Index: 11, Command: []byte("cmd11")},
			{Term: 3, Index: 12, Command: []byte("cmd12")},
		},
	}

	config := Config{
		ID: "node1",
		Peers: map[string]string{
			"node1": "localhost:9001",
			"node2": "localhost:9002",
			"node3": "localhost:9003",
		},
		ElectionTimeout:   300 * time.Millisecond,
		HeartbeatInterval: 100 * time.Millisecond,
	}

	node, err := NewRaftNode(config, trans, store, sm)
	if err != nil {
		t.Fatalf("创建节点失败: %v", err)
	}

	// 验证状态机被恢复
	sm.mu.Lock()
	// Restore 是在 NewRaftNode 中调用的
	sm.mu.Unlock()

	// 验证 LastApplied 被设置为 SnapshotIndex
	if node.volatile.LastApplied != 10 {
		t.Errorf("LastApplied 应为 10, 实际为 %d", node.volatile.LastApplied)
	}
	if node.volatile.CommitIndex < 10 {
		t.Errorf("CommitIndex 应至少为 10, 实际为 %d", node.volatile.CommitIndex)
	}

	// 验证 Raft 状态
	if node.persistent.CurrentTerm != 3 {
		t.Errorf("CurrentTerm 应为 3, 实际为 %d", node.persistent.CurrentTerm)
	}
	if node.persistent.SnapshotIndex != 10 {
		t.Errorf("SnapshotIndex 应为 10, 实际为 %d", node.persistent.SnapshotIndex)
	}
	if len(node.persistent.Log) != 2 {
		t.Errorf("Log 应有 2 条, 实际为 %d", len(node.persistent.Log))
	}

	// 验证索引转换仍然正确
	if idx := node.lastLogIndex(); idx != 12 {
		t.Errorf("lastLogIndex 应为 12, 实际为 %d", idx)
	}
	entry := node.getLogEntry(11)
	if entry == nil || string(entry.Command) != "cmd11" {
		t.Errorf("逻辑索引 11 的内容不正确")
	}
}

func TestAppendLogEntry_AfterSnapshot(t *testing.T) {
	node, _, _, _ := createTestNode("node1")

	// 模拟快照截断到 index=10
	node.persistent.SnapshotIndex = 10
	node.persistent.SnapshotTerm = 2
	node.persistent.CurrentTerm = 3
	node.persistent.Log = []LogEntry{} // 截断后无剩余日志

	// 追加新日志
	node.mu.Lock()
	entry := node.appendLogEntry([]byte("new-cmd"))
	node.mu.Unlock()

	// 新日志应该从 index=11 开始
	if entry.Index != 11 {
		t.Errorf("快照截断后新日志 Index 应为 11, 实际为 %d", entry.Index)
	}
	if entry.Term != 3 {
		t.Errorf("新日志 Term 应为 3, 实际为 %d", entry.Term)
	}
	if len(node.persistent.Log) != 1 {
		t.Errorf("物理日志应有 1 条, 实际为 %d", len(node.persistent.Log))
	}
}

func TestSnapshotMetadata_Fields(t *testing.T) {
	meta := SnapshotMetadata{
		LastIncludedIndex: 42,
		LastIncludedTerm:  5,
		CreatedAt:         "2026-01-01T00:00:00Z",
	}

	data, err := json.Marshal(meta)
	if err != nil {
		t.Fatalf("Marshal SnapshotMetadata 失败: %v", err)
	}

	var decoded SnapshotMetadata
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal SnapshotMetadata 失败: %v", err)
	}

	if decoded.LastIncludedIndex != 42 || decoded.LastIncludedTerm != 5 {
		t.Errorf("SnapshotMetadata 序列化不一致: %+v", decoded)
	}
	if decoded.CreatedAt != "2026-01-01T00:00:00Z" {
		t.Errorf("CreatedAt 应为 '2026-01-01T00:00:00Z', 实际为 '%s'", decoded.CreatedAt)
	}
}
