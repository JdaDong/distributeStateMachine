package server

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	pb "github.com/distributeStateMachine/proto"
	"github.com/distributeStateMachine/raft"
	"github.com/distributeStateMachine/service"
	"github.com/distributeStateMachine/statemachine"
	"github.com/distributeStateMachine/storage"
	"github.com/distributeStateMachine/transport"
)

// Server 封装 Raft 节点和 HTTP 服务
type Server struct {
	node        *raft.RaftNode
	kvStore     *statemachine.KVStore
	transport   *transport.HTTPTransport
	taskService *service.TaskService
	config      raft.Config
	httpAddr    string
}

// NewServer 创建服务器
func NewServer(config raft.Config, dataDir string) (*Server, error) {
	// 创建存储
	store, err := storage.NewFileStorage(dataDir)
	if err != nil {
		return nil, fmt.Errorf("create storage failed: %w", err)
	}

	// 创建状态机
	kvStore := statemachine.NewKVStore()

	// 创建传输层
	trans := transport.NewHTTPTransport(config.ListenAddr, nil)

	// 创建 Raft 节点
	node, err := raft.NewRaftNode(config, trans, store, kvStore)
	if err != nil {
		return nil, fmt.Errorf("create raft node failed: %w", err)
	}

	// 设置传输层处理器（回调到 Raft 节点）
	trans.SetHandler(node)

	// 创建 TaskService：node 实现了 Proposer 接口（Propose + GetLeaderID）
	// TaskService 通过 node.Propose 提交交易，由 Raft 共识保证一致性
	taskSvc := service.NewTaskService(node, kvStore)

	return &Server{
		node:        node,
		kvStore:     kvStore,
		transport:   trans,
		taskService: taskSvc,
		config:      config,
	}, nil
}

// Start 启动服务器
func (s *Server) Start() error {
	// 启动传输层
	if err := s.transport.Start(); err != nil {
		return fmt.Errorf("start transport failed: %w", err)
	}

	// 启动 Raft 节点
	s.node.Start()

	// 启动客户端 HTTP API
	go s.startClientAPI()

	log.Printf("[Server %s] 启动完成", s.config.ID)
	return nil
}

// Stop 停止服务器
func (s *Server) Stop() {
	s.node.Stop()
	s.transport.Stop()
}

// startClientAPI 启动客户端 API 服务
func (s *Server) startClientAPI() {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/set", s.handleSet)
	mux.HandleFunc("/api/get", s.handleGet)
	mux.HandleFunc("/api/delete", s.handleDelete)
	mux.HandleFunc("/api/status", s.handleStatus)

	// Task 业务层路由
	mux.HandleFunc("/api/task/set-status", s.handleSetTaskStatus)
	mux.HandleFunc("/api/task/get", s.handleGetTask)
	mux.HandleFunc("/api/task/list", s.handleListTasks)

	// 客户端 API 端口 = Raft 端口 + 1000
	_, port, _ := parseAddr(s.config.ListenAddr)
	apiAddr := fmt.Sprintf(":%d", port+1000)

	log.Printf("[Server %s] 客户端 API 监听在 %s", s.config.ID, apiAddr)
	if err := http.ListenAndServe(apiAddr, mux); err != nil {
		log.Printf("[Server %s] 客户端 API 错误: %v", s.config.ID, err)
	}
}

// handleSet 处理 SET 请求
func (s *Server) handleSet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeJSONError(w, "read body failed", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var req pb.ClientRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeJSONError(w, "decode failed", http.StatusBadRequest)
		return
	}

	cmd := raft.Command{
		Op:    "SET",
		Key:   req.Key,
		Value: req.Value,
	}
	cmdBytes, _ := raft.EncodeCommand(cmd)

	_, err = s.node.Propose(cmdBytes)
	if err != nil {
		if err.Error() == "not leader" {
			resp := pb.ClientResponse{
				Success:    false,
				Error:      "not leader",
				LeaderHint: s.node.GetLeaderID(),
			}
			writeJSON(w, resp)
			return
		}
		writeJSONError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeJSON(w, pb.ClientResponse{
		Success: true,
		Value:   "OK",
	})
}

// handleGet 处理 GET 请求
func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		writeJSONError(w, "key is required", http.StatusBadRequest)
		return
	}

	value, ok := s.kvStore.Get(key)
	if !ok {
		writeJSON(w, pb.ClientResponse{
			Success: false,
			Error:   "key not found",
		})
		return
	}

	writeJSON(w, pb.ClientResponse{
		Success: true,
		Value:   value,
	})
}

// handleDelete 处理 DELETE 请求
func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeJSONError(w, "read body failed", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var req pb.ClientRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeJSONError(w, "decode failed", http.StatusBadRequest)
		return
	}

	cmd := raft.Command{
		Op:  "DELETE",
		Key: req.Key,
	}
	cmdBytes, _ := raft.EncodeCommand(cmd)

	_, err = s.node.Propose(cmdBytes)
	if err != nil {
		if err.Error() == "not leader" {
			resp := pb.ClientResponse{
				Success:    false,
				Error:      "not leader",
				LeaderHint: s.node.GetLeaderID(),
			}
			writeJSON(w, resp)
			return
		}
		writeJSONError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeJSON(w, pb.ClientResponse{
		Success: true,
		Value:   "OK",
	})
}

// handleStatus 返回节点状态
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	term, isLeader := s.node.GetState()
	role := "Follower"
	if isLeader {
		role = "Leader"
	}

	status := map[string]interface{}{
		"id":        s.config.ID,
		"term":      term,
		"role":      role,
		"is_leader": isLeader,
		"peers":     s.config.Peers,
		"time":      time.Now().Format(time.RFC3339),
		"data":      s.kvStore.GetAll(),
	}

	writeJSON(w, status)
}

// 辅助函数
func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func writeJSONError(w http.ResponseWriter, msg string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(pb.ClientResponse{
		Success: false,
		Error:   msg,
	})
}

func parseAddr(addr string) (string, int, error) {
	var host string
	var port int
	_, err := fmt.Sscanf(addr, "%s:%d", &host, &port)
	if err != nil {
		// 尝试只有端口的情况
		_, err = fmt.Sscanf(addr, ":%d", &port)
		if err != nil {
			return "", 0, err
		}
		return "", port, nil
	}
	return host, port, nil
}

// =========================================================================
// Task 业务层 Handler
// =========================================================================

// handleSetTaskStatus 处理设置任务状态请求
func (s *Server) handleSetTaskStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeJSONError(w, "read body failed", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var req service.SetTaskStatusRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeJSONError(w, "decode failed: "+err.Error(), http.StatusBadRequest)
		return
	}

	resp, err := s.taskService.SetTaskStatus(&req)
	if err != nil {
		writeJSONError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// 补充 leader 提示
	if !resp.Success && resp.Error == "not leader" {
		leaderHint := s.taskService.GetLeaderHint()
		if leaderHint != "" {
			resp.Error = "not leader, leader_hint: " + leaderHint
		}
	}

	writeJSON(w, resp)
}

// handleGetTask 处理获取任务请求
func (s *Server) handleGetTask(w http.ResponseWriter, r *http.Request) {
	taskID := r.URL.Query().Get("task_id")
	if taskID == "" {
		writeJSONError(w, "task_id is required", http.StatusBadRequest)
		return
	}

	resp, err := s.taskService.GetTask(taskID)
	if err != nil {
		writeJSONError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeJSON(w, resp)
}

// handleListTasks 处理列出所有任务请求
func (s *Server) handleListTasks(w http.ResponseWriter, r *http.Request) {
	resp, err := s.taskService.ListTasks()
	if err != nil {
		writeJSONError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	writeJSON(w, resp)
}
