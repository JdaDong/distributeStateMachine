package transport

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	pb "github.com/distributeStateMachine/proto"
)

// mockRPCHandler 模拟 RPC 处理器
type mockRPCHandler struct {
	voteResp   *pb.RequestVoteResponse
	appendResp *pb.AppendEntriesResponse
}

func (m *mockRPCHandler) HandleRequestVote(req *pb.RequestVoteRequest) *pb.RequestVoteResponse {
	if m.voteResp != nil {
		return m.voteResp
	}
	return &pb.RequestVoteResponse{Term: req.Term, VoteGranted: true}
}

func (m *mockRPCHandler) HandleAppendEntries(req *pb.AppendEntriesRequest) *pb.AppendEntriesResponse {
	if m.appendResp != nil {
		return m.appendResp
	}
	return &pb.AppendEntriesResponse{Term: req.Term, Success: true, MatchIndex: 0}
}

func TestHTTPTransport_StartAndStop(t *testing.T) {
	handler := &mockRPCHandler{}
	trans := NewHTTPTransport(":0", handler) // :0 让系统分配随机端口

	err := trans.Start()
	if err != nil {
		t.Fatalf("启动失败: %v", err)
	}

	err = trans.Stop()
	if err != nil {
		t.Fatalf("停止失败: %v", err)
	}
}

func TestHTTPTransport_StopWithoutStart(t *testing.T) {
	trans := NewHTTPTransport(":0", nil)
	err := trans.Stop()
	if err != nil {
		t.Errorf("未启动时 Stop 不应报错: %v", err)
	}
}

func TestHTTPTransport_HandleRequestVote_HTTP(t *testing.T) {
	handler := &mockRPCHandler{
		voteResp: &pb.RequestVoteResponse{Term: 5, VoteGranted: true},
	}
	trans := NewHTTPTransport(":0", handler)

	// 构造请求
	req := pb.RequestVoteRequest{Term: 5, CandidateId: "node2", LastLogIndex: 0, LastLogTerm: 0}
	body, _ := json.Marshal(req)

	httpReq := httptest.NewRequest(http.MethodPost, "/raft/vote", strings.NewReader(string(body)))
	httpReq.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()

	trans.handleRequestVote(recorder, httpReq)

	if recorder.Code != http.StatusOK {
		t.Errorf("状态码应为 200, 实际为 %d", recorder.Code)
	}

	var resp pb.RequestVoteResponse
	json.Unmarshal(recorder.Body.Bytes(), &resp)

	if !resp.VoteGranted {
		t.Error("应返回 VoteGranted=true")
	}
	if resp.Term != 5 {
		t.Errorf("Term 应为 5, 实际为 %d", resp.Term)
	}
}

func TestHTTPTransport_HandleRequestVote_WrongMethod(t *testing.T) {
	trans := NewHTTPTransport(":0", &mockRPCHandler{})

	httpReq := httptest.NewRequest(http.MethodGet, "/raft/vote", nil)
	recorder := httptest.NewRecorder()

	trans.handleRequestVote(recorder, httpReq)

	if recorder.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET 请求应返回 405, 实际为 %d", recorder.Code)
	}
}

func TestHTTPTransport_HandleRequestVote_NoHandler(t *testing.T) {
	trans := NewHTTPTransport(":0", nil) // 无 handler

	req := pb.RequestVoteRequest{Term: 1, CandidateId: "node2"}
	body, _ := json.Marshal(req)

	httpReq := httptest.NewRequest(http.MethodPost, "/raft/vote", strings.NewReader(string(body)))
	recorder := httptest.NewRecorder()

	trans.handleRequestVote(recorder, httpReq)

	if recorder.Code != http.StatusServiceUnavailable {
		t.Errorf("无 handler 应返回 503, 实际为 %d", recorder.Code)
	}
}

func TestHTTPTransport_HandleAppendEntries_HTTP(t *testing.T) {
	handler := &mockRPCHandler{
		appendResp: &pb.AppendEntriesResponse{Term: 3, Success: true, MatchIndex: 5},
	}
	trans := NewHTTPTransport(":0", handler)

	req := pb.AppendEntriesRequest{
		Term:     3,
		LeaderId: "node1",
		Entries: []*pb.LogEntry{
			{Term: 3, Index: 5, Command: []byte("cmd")},
		},
		LeaderCommit: 4,
	}
	body, _ := json.Marshal(req)

	httpReq := httptest.NewRequest(http.MethodPost, "/raft/append", strings.NewReader(string(body)))
	httpReq.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()

	trans.handleAppendEntries(recorder, httpReq)

	if recorder.Code != http.StatusOK {
		t.Errorf("状态码应为 200, 实际为 %d", recorder.Code)
	}

	var resp pb.AppendEntriesResponse
	json.Unmarshal(recorder.Body.Bytes(), &resp)

	if !resp.Success {
		t.Error("应返回 Success=true")
	}
	if resp.MatchIndex != 5 {
		t.Errorf("MatchIndex 应为 5, 实际为 %d", resp.MatchIndex)
	}
}

func TestHTTPTransport_HandleAppendEntries_WrongMethod(t *testing.T) {
	trans := NewHTTPTransport(":0", &mockRPCHandler{})

	httpReq := httptest.NewRequest(http.MethodGet, "/raft/append", nil)
	recorder := httptest.NewRecorder()

	trans.handleAppendEntries(recorder, httpReq)

	if recorder.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET 请求应返回 405, 实际为 %d", recorder.Code)
	}
}

func TestHTTPTransport_HandleAppendEntries_InvalidJSON(t *testing.T) {
	trans := NewHTTPTransport(":0", &mockRPCHandler{})

	httpReq := httptest.NewRequest(http.MethodPost, "/raft/append", strings.NewReader("bad json"))
	recorder := httptest.NewRecorder()

	trans.handleAppendEntries(recorder, httpReq)

	if recorder.Code != http.StatusBadRequest {
		t.Errorf("无效 JSON 应返回 400, 实际为 %d", recorder.Code)
	}
}

func TestHTTPTransport_HandleRequestVote_InvalidJSON(t *testing.T) {
	trans := NewHTTPTransport(":0", &mockRPCHandler{})

	httpReq := httptest.NewRequest(http.MethodPost, "/raft/vote", strings.NewReader("bad json"))
	recorder := httptest.NewRecorder()

	trans.handleRequestVote(recorder, httpReq)

	if recorder.Code != http.StatusBadRequest {
		t.Errorf("无效 JSON 应返回 400, 实际为 %d", recorder.Code)
	}
}

func TestHTTPTransport_SetHandler(t *testing.T) {
	trans := NewHTTPTransport(":0", nil)

	handler := &mockRPCHandler{
		voteResp: &pb.RequestVoteResponse{Term: 1, VoteGranted: true},
	}
	trans.SetHandler(handler)

	// 验证可以正常处理请求
	req := pb.RequestVoteRequest{Term: 1, CandidateId: "node1"}
	body, _ := json.Marshal(req)
	httpReq := httptest.NewRequest(http.MethodPost, "/raft/vote", strings.NewReader(string(body)))
	recorder := httptest.NewRecorder()

	trans.handleRequestVote(recorder, httpReq)

	if recorder.Code != http.StatusOK {
		t.Errorf("设置 handler 后应正常处理, 状态码 %d", recorder.Code)
	}
}

func TestHTTPTransport_EndToEnd_RequestVote(t *testing.T) {
	handler := &mockRPCHandler{
		voteResp: &pb.RequestVoteResponse{Term: 1, VoteGranted: true},
	}
	trans := NewHTTPTransport(":18901", handler)
	err := trans.Start()
	if err != nil {
		t.Fatalf("启动传输层失败: %v", err)
	}
	defer trans.Stop()

	// 等待服务器就绪
	time.Sleep(50 * time.Millisecond)

	// 通过传输层发送 RPC
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	req := &pb.RequestVoteRequest{
		Term:        1,
		CandidateId: "node2",
	}

	resp, err := trans.SendRequestVote(ctx, "localhost:18901", req)
	if err != nil {
		t.Fatalf("SendRequestVote 失败: %v", err)
	}
	if !resp.VoteGranted {
		t.Error("应返回 VoteGranted=true")
	}
}

func TestHTTPTransport_EndToEnd_AppendEntries(t *testing.T) {
	handler := &mockRPCHandler{
		appendResp: &pb.AppendEntriesResponse{Term: 1, Success: true, MatchIndex: 1},
	}
	trans := NewHTTPTransport(":18902", handler)
	err := trans.Start()
	if err != nil {
		t.Fatalf("启动传输层失败: %v", err)
	}
	defer trans.Stop()

	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	req := &pb.AppendEntriesRequest{
		Term:     1,
		LeaderId: "node1",
		Entries: []*pb.LogEntry{
			{Term: 1, Index: 1, Command: []byte("cmd1")},
		},
	}

	resp, err := trans.SendAppendEntries(ctx, "localhost:18902", req)
	if err != nil {
		t.Fatalf("SendAppendEntries 失败: %v", err)
	}
	if !resp.Success {
		t.Error("应返回 Success=true")
	}
	if resp.MatchIndex != 1 {
		t.Errorf("MatchIndex 应为 1, 实际为 %d", resp.MatchIndex)
	}
}

func TestHTTPTransport_SendToUnavailableTarget(t *testing.T) {
	trans := NewHTTPTransport(":0", nil)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	req := &pb.RequestVoteRequest{Term: 1, CandidateId: "node1"}
	_, err := trans.SendRequestVote(ctx, "localhost:19999", req)
	if err == nil {
		t.Error("连接不可达的目标应返回错误")
	}
}

// =========================================================================
// bytesReader 测试
// =========================================================================

func TestBytesReader(t *testing.T) {
	data := []byte("hello world")
	reader := newBytesReader(data)

	buf := make([]byte, 5)
	n, err := reader.Read(buf)
	if err != nil {
		t.Fatalf("第一次读取不应报错: %v", err)
	}
	if n != 5 {
		t.Errorf("第一次读取应返回 5 字节, 实际为 %d", n)
	}
	if string(buf) != "hello" {
		t.Errorf("第一次读取内容应为 'hello', 实际为 '%s'", buf)
	}

	buf = make([]byte, 20)
	n, err = reader.Read(buf)
	if err != nil {
		t.Fatalf("第二次读取不应报错: %v", err)
	}
	if n != 6 {
		t.Errorf("第二次读取应返回 6 字节, 实际为 %d", n)
	}
	if string(buf[:n]) != " world" {
		t.Errorf("第二次读取内容应为 ' world', 实际为 '%s'", buf[:n])
	}

	// 读完后应返回 EOF
	n, err = reader.Read(buf)
	if n != 0 {
		t.Errorf("读完后应返回 0 字节, 实际为 %d", n)
	}
	if err == nil {
		t.Error("读完后应返回 EOF 错误")
	}
}
