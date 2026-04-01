package transport

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	pb "github.com/distributeStateMachine/proto"
)

// RPCHandler 定义 RPC 请求处理接口
type RPCHandler interface {
	HandleRequestVote(req *pb.RequestVoteRequest) *pb.RequestVoteResponse
	HandleAppendEntries(req *pb.AppendEntriesRequest) *pb.AppendEntriesResponse
}

// HTTPTransport 基于 HTTP JSON 的传输层实现
type HTTPTransport struct {
	mu         sync.RWMutex
	listenAddr string
	handler    RPCHandler
	server     *http.Server
	client     *http.Client
}

// NewHTTPTransport 创建 HTTP 传输层
func NewHTTPTransport(listenAddr string, handler RPCHandler) *HTTPTransport {
	return &HTTPTransport{
		listenAddr: listenAddr,
		handler:    handler,
		client: &http.Client{
			Timeout: 2 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 10,
				IdleConnTimeout:     30 * time.Second,
			},
		},
	}
}

// SetHandler 设置 RPC 处理器
func (t *HTTPTransport) SetHandler(handler RPCHandler) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.handler = handler
}

// Start 启动 HTTP 服务器
func (t *HTTPTransport) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/raft/vote", t.handleRequestVote)
	mux.HandleFunc("/raft/append", t.handleAppendEntries)

	t.server = &http.Server{
		Addr:    t.listenAddr,
		Handler: mux,
	}

	ln, err := net.Listen("tcp", t.listenAddr)
	if err != nil {
		return fmt.Errorf("listen failed: %w", err)
	}

	go func() {
		if err := t.server.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP server error: %v", err)
		}
	}()

	log.Printf("Transport listening on %s", t.listenAddr)
	return nil
}

// Stop 停止 HTTP 服务器
func (t *HTTPTransport) Stop() error {
	if t.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return t.server.Shutdown(ctx)
	}
	return nil
}

// handleRequestVote 处理 RequestVote HTTP 请求
func (t *HTTPTransport) handleRequestVote(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body failed", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var req pb.RequestVoteRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "decode failed", http.StatusBadRequest)
		return
	}

	t.mu.RLock()
	handler := t.handler
	t.mu.RUnlock()

	if handler == nil {
		http.Error(w, "no handler", http.StatusServiceUnavailable)
		return
	}

	resp := handler.HandleRequestVote(&req)
	data, _ := json.Marshal(resp)
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

// handleAppendEntries 处理 AppendEntries HTTP 请求
func (t *HTTPTransport) handleAppendEntries(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body failed", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var req pb.AppendEntriesRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "decode failed", http.StatusBadRequest)
		return
	}

	t.mu.RLock()
	handler := t.handler
	t.mu.RUnlock()

	if handler == nil {
		http.Error(w, "no handler", http.StatusServiceUnavailable)
		return
	}

	resp := handler.HandleAppendEntries(&req)
	data, _ := json.Marshal(resp)
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

// SendRequestVote 发送 RequestVote RPC
func (t *HTTPTransport) SendRequestVote(ctx context.Context, target string, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	data, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	url := fmt.Sprintf("http://%s/raft/vote", target)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Body = io.NopCloser(newBytesReader(data))
	httpReq.ContentLength = int64(len(data))

	httpResp, err := t.client.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer httpResp.Body.Close()

	respBody, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return nil, err
	}

	var resp pb.RequestVoteResponse
	if err := json.Unmarshal(respBody, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// SendAppendEntries 发送 AppendEntries RPC
func (t *HTTPTransport) SendAppendEntries(ctx context.Context, target string, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	data, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	url := fmt.Sprintf("http://%s/raft/append", target)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Body = io.NopCloser(newBytesReader(data))
	httpReq.ContentLength = int64(len(data))

	httpResp, err := t.client.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer httpResp.Body.Close()

	respBody, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return nil, err
	}

	var resp pb.AppendEntriesResponse
	if err := json.Unmarshal(respBody, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// bytesReader 简单的字节读取器
type bytesReader struct {
	data []byte
	pos  int
}

func newBytesReader(data []byte) *bytesReader {
	return &bytesReader{data: data}
}

func (r *bytesReader) Read(p []byte) (n int, err error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n = copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}
