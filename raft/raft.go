package raft

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/distributeStateMachine/proto"
)

// NewRaftNode 创建一个新的 Raft 节点
func NewRaftNode(config Config, transport Transport, storage Storage, sm StateMachine) (*RaftNode, error) {
	node := &RaftNode{
		config:    config,
		state:     Follower,
		transport: transport,
		storage:   storage,
		persistent: PersistentState{
			CurrentTerm: 0,
			VotedFor:    "",
			Log:         make([]LogEntry, 0),
		},
		volatile: VolatileState{
			CommitIndex: 0,
			LastApplied: 0,
		},
		leaderState: LeaderState{
			NextIndex:  make(map[string]uint64),
			MatchIndex: make(map[string]uint64),
		},
		applyCh:          make(chan ApplyMsg, 100),
		stopCh:           make(chan struct{}),
		resetTimerCh:     make(chan struct{}, 1),
		pendingProposals: make(map[uint64]*pendingProposal),
		stateMachine:     sm,
	}

	// 步骤 1：尝试从快照恢复状态机
	snapMeta, snapData, err := storage.LoadSnapshot()
	if err != nil {
		log.Printf("[%s] 加载快照失败(忽略): %v", config.ID, err)
	} else if snapMeta != nil && snapData != nil {
		if err := sm.Restore(snapData); err != nil {
			return nil, fmt.Errorf("restore state machine from snapshot failed: %w", err)
		}
		log.Printf("[%s] 从快照恢复状态机: index=%d, term=%d",
			config.ID, snapMeta.LastIncludedIndex, snapMeta.LastIncludedTerm)
	}

	// 步骤 2：从持久化存储中恢复 Raft 状态（日志 + 元数据）
	savedState, err := storage.LoadState()
	if err == nil && savedState != nil {
		node.persistent = *savedState
		log.Printf("[%s] 从持久化存储恢复状态: term=%d, log_len=%d, snapshot_index=%d",
			config.ID, savedState.CurrentTerm, len(savedState.Log), savedState.SnapshotIndex)
	}

	// 步骤 3：设置已应用索引（快照已包含的数据不需要再次 Apply）
	if node.persistent.SnapshotIndex > 0 {
		node.volatile.LastApplied = node.persistent.SnapshotIndex
		// commitIndex 至少要从快照截断点开始
		if node.volatile.CommitIndex < node.persistent.SnapshotIndex {
			node.volatile.CommitIndex = node.persistent.SnapshotIndex
		}
	}

	return node, nil
}

// Start 启动 Raft 节点
func (rn *RaftNode) Start() {
	log.Printf("[%s] 节点启动，初始角色: %s", rn.config.ID, rn.state)
	go rn.electionLoop()
	go rn.applyLoop()
}

// Stop 停止 Raft 节点
func (rn *RaftNode) Stop() {
	close(rn.stopCh)
}

// GetState 获取当前节点状态
func (rn *RaftNode) GetState() (uint64, bool) {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.persistent.CurrentTerm, rn.state == Leader
}

// GetLeaderID 返回当前已知的 Leader ID (简化实现)
func (rn *RaftNode) GetLeaderID() string {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	if rn.state == Leader {
		return rn.config.ID
	}
	return ""
}

// =========================================================================
// 选举相关
// =========================================================================

// electionLoop 选举循环：管理选举超时和触发选举
func (rn *RaftNode) electionLoop() {
	timeout := rn.randomElectionTimeout()
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-rn.stopCh:
			return
		case <-timer.C:
			rn.mu.Lock()
			if rn.state != Leader {
				rn.startElection()
			}
			rn.mu.Unlock()
			timer.Reset(rn.randomElectionTimeout())
		case <-rn.resetTimerCh:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(rn.randomElectionTimeout())
		}
	}
}

// randomElectionTimeout 返回一个随机选举超时时间
func (rn *RaftNode) randomElectionTimeout() time.Duration {
	base := rn.config.ElectionTimeout
	if base == 0 {
		base = 300 * time.Millisecond
	}
	// 超时时间为 [base, 2*base) 之间的随机值
	return base + time.Duration(rand.Int63n(int64(base)))
}

// startElection 发起选举（调用时必须持有锁）
func (rn *RaftNode) startElection() {
	rn.persistent.CurrentTerm++
	rn.state = Candidate
	rn.persistent.VotedFor = rn.config.ID
	currentTerm := rn.persistent.CurrentTerm

	log.Printf("[%s] 发起选举，任期: %d", rn.config.ID, currentTerm)

	// 保存状态
	rn.saveState()

	lastLogIndex, lastLogTerm := rn.lastLogInfo()

	req := &pb.RequestVoteRequest{
		Term:         currentTerm,
		CandidateId:  rn.config.ID,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	// 自身投票给自己，从 1 票开始计数
	var votesReceived int32 = 1
	majority := len(rn.config.Peers)/2 + 1

	var wg sync.WaitGroup
	for peerID, peerAddr := range rn.config.Peers {
		if peerID == rn.config.ID {
			continue
		}
		wg.Add(1)
		go func(id, addr string) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()

			resp, err := rn.transport.SendRequestVote(ctx, addr, req)
			if err != nil {
				log.Printf("[%s] 向 %s 请求投票失败: %v", rn.config.ID, id, err)
				return
			}

			rn.mu.Lock()
			defer rn.mu.Unlock()

			// 如果响应的任期更大，退回 Follower
			if resp.Term > rn.persistent.CurrentTerm {
				rn.becomeFollower(resp.Term)
				return
			}

			// 如果已经不是当前任期的 Candidate 了，忽略
			if rn.state != Candidate || rn.persistent.CurrentTerm != currentTerm {
				return
			}

			if resp.VoteGranted {
				votes := atomic.AddInt32(&votesReceived, 1)
				if int(votes) >= majority {
					rn.becomeLeader()
				}
			}
		}(peerID, peerAddr)
	}

	// 不阻塞等待所有响应，选举在获得多数票后立即生效
}

// becomeFollower 转换为 Follower 角色（调用时必须持有锁）
func (rn *RaftNode) becomeFollower(term uint64) {
	log.Printf("[%s] 转为 Follower, 任期: %d -> %d", rn.config.ID, rn.persistent.CurrentTerm, term)
	rn.state = Follower
	rn.persistent.CurrentTerm = term
	rn.persistent.VotedFor = ""
	rn.saveState()
}

// becomeLeader 转换为 Leader 角色（调用时必须持有锁）
func (rn *RaftNode) becomeLeader() {
	if rn.state == Leader {
		return
	}
	log.Printf("[%s] 当选 Leader, 任期: %d", rn.config.ID, rn.persistent.CurrentTerm)
	rn.state = Leader

	// 初始化 Leader 状态
	lastIdx := rn.lastLogIndex()
	for peerID := range rn.config.Peers {
		if peerID == rn.config.ID {
			continue
		}
		rn.leaderState.NextIndex[peerID] = lastIdx + 1
		rn.leaderState.MatchIndex[peerID] = 0
	}

	// 启动心跳
	go rn.heartbeatLoop()

	// 发送一个空的 no-op 日志来提交之前任期的日志
	rn.appendLogEntry(nil)
}

// =========================================================================
// RequestVote RPC 处理
// =========================================================================

// HandleRequestVote 处理 RequestVote RPC
func (rn *RaftNode) HandleRequestVote(req *pb.RequestVoteRequest) *pb.RequestVoteResponse {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	resp := &pb.RequestVoteResponse{
		Term:        rn.persistent.CurrentTerm,
		VoteGranted: false,
	}

	// 如果请求的任期小于当前任期，拒绝
	if req.Term < rn.persistent.CurrentTerm {
		return resp
	}

	// 如果请求的任期更大，转为 Follower
	if req.Term > rn.persistent.CurrentTerm {
		rn.becomeFollower(req.Term)
	}

	resp.Term = rn.persistent.CurrentTerm

	// 检查是否已投票
	if rn.persistent.VotedFor != "" && rn.persistent.VotedFor != req.CandidateId {
		return resp
	}

	// 检查候选人的日志是否至少和自己一样新
	lastLogIndex, lastLogTerm := rn.lastLogInfo()
	if req.LastLogTerm < lastLogTerm ||
		(req.LastLogTerm == lastLogTerm && req.LastLogIndex < lastLogIndex) {
		return resp
	}

	// 投票给候选人
	rn.persistent.VotedFor = req.CandidateId
	rn.saveState()
	resp.VoteGranted = true

	// 重置选举计时器
	rn.resetElectionTimer()

	log.Printf("[%s] 投票给 %s, 任期: %d", rn.config.ID, req.CandidateId, rn.persistent.CurrentTerm)
	return resp
}

// =========================================================================
// AppendEntries RPC（日志复制 + 心跳）
// =========================================================================

// HandleAppendEntries 处理 AppendEntries RPC
func (rn *RaftNode) HandleAppendEntries(req *pb.AppendEntriesRequest) *pb.AppendEntriesResponse {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	resp := &pb.AppendEntriesResponse{
		Term:    rn.persistent.CurrentTerm,
		Success: false,
	}

	// 如果请求的任期小于当前任期，拒绝
	if req.Term < rn.persistent.CurrentTerm {
		return resp
	}

	// 重置选举计时器（收到合法 Leader 的消息）
	rn.resetElectionTimer()

	// 如果请求的任期更大或等于，且当前不是 Follower，转为 Follower
	if req.Term > rn.persistent.CurrentTerm || rn.state != Follower {
		rn.becomeFollower(req.Term)
	}
	resp.Term = rn.persistent.CurrentTerm

	// 日志一致性检查（使用逻辑索引）
	if req.PrevLogIndex > 0 {
		// 如果 prevLogIndex 在快照范围内，说明已经匹配（快照中的数据是一致的）
		if req.PrevLogIndex <= rn.persistent.SnapshotIndex {
			// 快照中的数据一定是正确的，跳过一致性检查
		} else if req.PrevLogIndex > rn.lastLogIndex() {
			// 日志太短，不匹配
			resp.MatchIndex = rn.lastLogIndex()
			return resp
		} else {
			prevEntry := rn.getLogEntry(req.PrevLogIndex)
			if prevEntry == nil || prevEntry.Term != req.PrevLogTerm {
				// 任期不匹配，需要回退
				physical := rn.logicalToPhysical(req.PrevLogIndex)
				if physical >= 0 && physical < len(rn.persistent.Log) {
					rn.persistent.Log = rn.persistent.Log[:physical]
				}
				rn.saveState()
				resp.MatchIndex = rn.lastLogIndex()
				return resp
			}
		}
	}

	// 追加新日志条目
	if len(req.Entries) > 0 {
		for _, entry := range req.Entries {
			logEntry := LogEntryFromProto(entry)

			// 跳过已被快照覆盖的日志
			if logEntry.Index <= rn.persistent.SnapshotIndex {
				continue
			}

			physical := rn.logicalToPhysical(logEntry.Index)
			if physical < len(rn.persistent.Log) {
				// 已存在
				existing := rn.persistent.Log[physical]
				if existing.Term != logEntry.Term {
					// 冲突，截断并追加
					rn.persistent.Log = rn.persistent.Log[:physical]
					rn.persistent.Log = append(rn.persistent.Log, logEntry)
				}
			} else {
				rn.persistent.Log = append(rn.persistent.Log, logEntry)
			}
		}
		rn.saveState()
	}

	// 更新提交索引
	if req.LeaderCommit > rn.volatile.CommitIndex {
		lastNewIndex := rn.lastLogIndex()
		if len(req.Entries) > 0 {
			lastNewIndex = req.Entries[len(req.Entries)-1].Index
		}
		if req.LeaderCommit < lastNewIndex {
			rn.volatile.CommitIndex = req.LeaderCommit
		} else {
			rn.volatile.CommitIndex = lastNewIndex
		}
	}

	resp.Success = true
	resp.MatchIndex = rn.lastLogIndex()
	return resp
}

// =========================================================================
// 心跳与日志复制
// =========================================================================

// heartbeatLoop Leader 心跳循环
func (rn *RaftNode) heartbeatLoop() {
	interval := rn.config.HeartbeatInterval
	if interval == 0 {
		interval = 100 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// 立即发送一次心跳
	rn.broadcastAppendEntries()

	for {
		select {
		case <-rn.stopCh:
			return
		case <-ticker.C:
			rn.mu.RLock()
			isLeader := rn.state == Leader
			rn.mu.RUnlock()

			if !isLeader {
				return
			}
			rn.broadcastAppendEntries()
		}
	}
}

// broadcastAppendEntries 向所有 Follower 发送 AppendEntries
func (rn *RaftNode) broadcastAppendEntries() {
	rn.mu.RLock()
	if rn.state != Leader {
		rn.mu.RUnlock()
		return
	}
	currentTerm := rn.persistent.CurrentTerm
	commitIndex := rn.volatile.CommitIndex
	rn.mu.RUnlock()

	for peerID, peerAddr := range rn.config.Peers {
		if peerID == rn.config.ID {
			continue
		}
		go rn.sendAppendEntries(peerID, peerAddr, currentTerm, commitIndex)
	}
}

// sendAppendEntries 向单个 Follower 发送 AppendEntries
func (rn *RaftNode) sendAppendEntries(peerID, peerAddr string, term, commitIndex uint64) {
	rn.mu.RLock()
	nextIndex := rn.leaderState.NextIndex[peerID]
	prevLogIndex := uint64(0)
	prevLogTerm := uint64(0)

	if nextIndex > 1 {
		prevLogIndex = nextIndex - 1
		// 如果 prev 在快照范围内
		if prevLogIndex <= rn.persistent.SnapshotIndex {
			if prevLogIndex == rn.persistent.SnapshotIndex {
				prevLogTerm = rn.persistent.SnapshotTerm
			}
			// 如果 nextIndex 也在快照范围内或刚好等于 SnapshotIndex+1
			// 从 SnapshotIndex+1 开始发送
		} else {
			entry := rn.getLogEntry(prevLogIndex)
			if entry != nil {
				prevLogTerm = entry.Term
			}
		}
	}

	// 收集需要发送的日志条目（逻辑索引）
	var entries []*pb.LogEntry
	lastIdx := rn.lastLogIndex()
	if nextIndex <= lastIdx {
		// 确保 nextIndex 在可用范围内
		start := nextIndex
		if start <= rn.persistent.SnapshotIndex {
			start = rn.persistent.SnapshotIndex + 1
		}
		for i := start; i <= lastIdx; i++ {
			entry := rn.getLogEntry(i)
			if entry != nil {
				entries = append(entries, entry.ToProto())
			}
		}
	}
	rn.mu.RUnlock()

	req := &pb.AppendEntriesRequest{
		Term:         term,
		LeaderId:     rn.config.ID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: commitIndex,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	resp, err := rn.transport.SendAppendEntries(ctx, peerAddr, req)
	if err != nil {
		return
	}

	rn.mu.Lock()
	defer rn.mu.Unlock()

	// 如果响应的任期更大，退回 Follower
	if resp.Term > rn.persistent.CurrentTerm {
		rn.becomeFollower(resp.Term)
		return
	}

	// 已不是 Leader 或任期已变化
	if rn.state != Leader || rn.persistent.CurrentTerm != term {
		return
	}

	if resp.Success {
		// 更新 matchIndex 和 nextIndex
		if len(entries) > 0 {
			newMatchIndex := entries[len(entries)-1].Index
			if newMatchIndex > rn.leaderState.MatchIndex[peerID] {
				rn.leaderState.MatchIndex[peerID] = newMatchIndex
				rn.leaderState.NextIndex[peerID] = newMatchIndex + 1
			}
		}
		// 尝试推进 commitIndex
		rn.advanceCommitIndex()
	} else {
		// 回退 nextIndex
		if resp.MatchIndex > 0 {
			rn.leaderState.NextIndex[peerID] = resp.MatchIndex + 1
		} else if rn.leaderState.NextIndex[peerID] > 1 {
			rn.leaderState.NextIndex[peerID]--
		}
		// 不要回退到快照范围以内
		if rn.leaderState.NextIndex[peerID] <= rn.persistent.SnapshotIndex {
			rn.leaderState.NextIndex[peerID] = rn.persistent.SnapshotIndex + 1
		}
	}
}

// advanceCommitIndex 尝试推进 commitIndex（调用时必须持有锁）
func (rn *RaftNode) advanceCommitIndex() {
	// 收集所有 matchIndex
	matchIndexes := make([]uint64, 0, len(rn.config.Peers))
	for peerID := range rn.config.Peers {
		if peerID == rn.config.ID {
			// Leader 自身的 matchIndex 是日志末尾
			matchIndexes = append(matchIndexes, rn.lastLogIndex())
		} else {
			matchIndexes = append(matchIndexes, rn.leaderState.MatchIndex[peerID])
		}
	}

	// 排序后取中位数
	sort.Slice(matchIndexes, func(i, j int) bool {
		return matchIndexes[i] > matchIndexes[j] // 降序排列
	})

	majority := len(rn.config.Peers) / 2
	newCommitIndex := matchIndexes[majority]

	// 只能提交当前任期的日志
	if newCommitIndex > rn.volatile.CommitIndex {
		entry := rn.getLogEntry(newCommitIndex)
		if entry != nil && entry.Term == rn.persistent.CurrentTerm {
			rn.volatile.CommitIndex = newCommitIndex
			log.Printf("[%s] commitIndex 推进到 %d", rn.config.ID, newCommitIndex)
		}
	}
}

// =========================================================================
// 客户端提案
// =========================================================================

// Propose 处理客户端的写请求
func (rn *RaftNode) Propose(command []byte) (ApplyMsg, error) {
	rn.mu.Lock()

	if rn.state != Leader {
		rn.mu.Unlock()
		return ApplyMsg{}, fmt.Errorf("not leader")
	}

	// 追加日志
	entry := rn.appendLogEntry(command)

	// 创建 pending proposal
	proposal := &pendingProposal{
		index:  entry.Index,
		term:   entry.Term,
		result: make(chan ApplyMsg, 1),
	}
	rn.pendingProposals[entry.Index] = proposal
	rn.mu.Unlock()

	// 立即触发日志复制
	rn.broadcastAppendEntries()

	// 等待提交结果
	select {
	case result := <-proposal.result:
		return result, nil
	case <-time.After(5 * time.Second):
		rn.mu.Lock()
		delete(rn.pendingProposals, entry.Index)
		rn.mu.Unlock()
		return ApplyMsg{}, fmt.Errorf("proposal timeout")
	case <-rn.stopCh:
		return ApplyMsg{}, fmt.Errorf("node stopped")
	}
}

// appendLogEntry 追加一条日志（调用时必须持有锁）
func (rn *RaftNode) appendLogEntry(command []byte) LogEntry {
	entry := LogEntry{
		Term:    rn.persistent.CurrentTerm,
		Index:   rn.lastLogIndex() + 1,
		Command: command,
	}
	rn.persistent.Log = append(rn.persistent.Log, entry)
	rn.saveState()
	return entry
}

// =========================================================================
// 应用循环
// =========================================================================

// applyLoop 将已提交的日志应用到状态机
func (rn *RaftNode) applyLoop() {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-rn.stopCh:
			return
		case <-ticker.C:
			rn.applyCommitted()
		}
	}
}

// applyCommitted 应用所有已提交但未应用的日志
func (rn *RaftNode) applyCommitted() {
	rn.mu.Lock()
	commitIndex := rn.volatile.CommitIndex
	lastApplied := rn.volatile.LastApplied

	if commitIndex <= lastApplied {
		rn.mu.Unlock()
		return
	}

	// 复制待应用的日志（使用逻辑索引）
	entries := make([]LogEntry, 0, commitIndex-lastApplied)
	for i := lastApplied + 1; i <= commitIndex; i++ {
		entry := rn.getLogEntry(i)
		if entry != nil {
			entries = append(entries, *entry)
		}
	}
	rn.mu.Unlock()

	// 逐条应用到状态机
	for _, entry := range entries {
		if entry.Command != nil {
			_, err := rn.stateMachine.Apply(entry.Command)
			if err != nil {
				log.Printf("[%s] 应用日志 %d 失败: %v", rn.config.ID, entry.Index, err)
			}
		}

		msg := ApplyMsg{
			CommandValid: entry.Command != nil,
			Command:      entry.Command,
			CommandIndex: entry.Index,
			CommandTerm:  entry.Term,
		}

		rn.mu.Lock()
		rn.volatile.LastApplied = entry.Index

		// 通知等待的客户端
		if proposal, ok := rn.pendingProposals[entry.Index]; ok {
			if proposal.term == entry.Term {
				proposal.result <- msg
			}
			delete(rn.pendingProposals, entry.Index)
		}
		rn.mu.Unlock()
	}

	// 应用完成后检查是否需要触发快照
	if len(entries) > 0 {
		rn.maybeSnapshot()
	}
}

// =========================================================================
// 辅助方法
// =========================================================================

// lastLogInfo 返回最后一条日志的索引和任期（调用时必须持有锁）
func (rn *RaftNode) lastLogInfo() (uint64, uint64) {
	if len(rn.persistent.Log) == 0 {
		// 没有日志条目时，返回快照截断点
		return rn.persistent.SnapshotIndex, rn.persistent.SnapshotTerm
	}
	last := rn.persistent.Log[len(rn.persistent.Log)-1]
	return last.Index, last.Term
}

// logicalToPhysical 将逻辑索引（全局日志索引）转换为物理数组下标
// 快照截断后，逻辑索引 N 对应物理数组 [N - SnapshotIndex - 1]
func (rn *RaftNode) logicalToPhysical(logicalIndex uint64) int {
	return int(logicalIndex - rn.persistent.SnapshotIndex - 1)
}

// physicalToLogical 将物理数组下标转换为逻辑索引
func (rn *RaftNode) physicalToLogical(physicalIndex int) uint64 {
	return uint64(physicalIndex) + rn.persistent.SnapshotIndex + 1
}

// getLogEntry 通过逻辑索引获取日志条目（调用时必须持有锁）
// 如果索引在快照范围内或超出日志范围，返回 nil
func (rn *RaftNode) getLogEntry(logicalIndex uint64) *LogEntry {
	if logicalIndex <= rn.persistent.SnapshotIndex {
		return nil // 已被快照覆盖
	}
	physical := rn.logicalToPhysical(logicalIndex)
	if physical < 0 || physical >= len(rn.persistent.Log) {
		return nil
	}
	entry := rn.persistent.Log[physical]
	return &entry
}

// lastLogIndex 返回最后一条日志的逻辑索引（调用时必须持有锁）
func (rn *RaftNode) lastLogIndex() uint64 {
	if len(rn.persistent.Log) == 0 {
		return rn.persistent.SnapshotIndex
	}
	return rn.persistent.Log[len(rn.persistent.Log)-1].Index
}

// resetElectionTimer 重置选举计时器
func (rn *RaftNode) resetElectionTimer() {
	select {
	case rn.resetTimerCh <- struct{}{}:
	default:
	}
}

// saveState 保存持久化状态
func (rn *RaftNode) saveState() {
	if err := rn.storage.SaveState(&rn.persistent); err != nil {
		log.Printf("[%s] 保存状态失败: %v", rn.config.ID, err)
	}
}

// =========================================================================
// 快照与日志压缩
// =========================================================================

// maybeSnapshot 检查是否需要触发快照
// 当日志条目数超过阈值时，对状态机做快照并截断旧日志
func (rn *RaftNode) maybeSnapshot() {
	threshold := rn.config.SnapshotThreshold
	if threshold == 0 {
		threshold = 100 // 默认 100 条日志触发一次快照
	}

	rn.mu.RLock()
	logLen := uint64(len(rn.persistent.Log))
	lastApplied := rn.volatile.LastApplied
	snapshotIndex := rn.persistent.SnapshotIndex
	rn.mu.RUnlock()

	if logLen < threshold {
		return
	}

	// 只能对已应用到状态机的日志做快照
	if lastApplied <= snapshotIndex {
		return
	}

	log.Printf("[%s] 日志条目数 %d 达到阈值 %d，触发快照 (lastApplied=%d)",
		rn.config.ID, logLen, threshold, lastApplied)

	// 1. 获取状态机快照
	snapData, err := rn.stateMachine.Snapshot()
	if err != nil {
		log.Printf("[%s] 获取状态机快照失败: %v", rn.config.ID, err)
		return
	}

	rn.mu.Lock()
	defer rn.mu.Unlock()

	// 再次检查（可能有并发变化）
	if rn.volatile.LastApplied <= rn.persistent.SnapshotIndex {
		return
	}

	// 2. 确定快照截断点
	snapIndex := rn.volatile.LastApplied
	snapEntry := rn.getLogEntry(snapIndex)
	if snapEntry == nil {
		log.Printf("[%s] 快照截断点 %d 对应的日志不存在", rn.config.ID, snapIndex)
		return
	}
	snapTerm := snapEntry.Term

	// 3. 保存快照到磁盘
	metadata := SnapshotMetadata{
		LastIncludedIndex: snapIndex,
		LastIncludedTerm:  snapTerm,
		CreatedAt:         time.Now().Format(time.RFC3339),
	}
	if err := rn.storage.SaveSnapshot(metadata, snapData); err != nil {
		log.Printf("[%s] 保存快照到磁盘失败: %v", rn.config.ID, err)
		return
	}

	// 4. 截断旧日志：保留 snapIndex 之后的日志
	physical := rn.logicalToPhysical(snapIndex)
	if physical+1 < len(rn.persistent.Log) {
		remaining := make([]LogEntry, len(rn.persistent.Log)-physical-1)
		copy(remaining, rn.persistent.Log[physical+1:])
		rn.persistent.Log = remaining
	} else {
		rn.persistent.Log = make([]LogEntry, 0)
	}

	// 5. 更新快照截断点
	rn.persistent.SnapshotIndex = snapIndex
	rn.persistent.SnapshotTerm = snapTerm

	// 6. 持久化 Raft 状态（截断后的日志 + 新的 SnapshotIndex）
	rn.saveState()

	log.Printf("[%s] 日志压缩完成: snapshot_index=%d, snapshot_term=%d, remaining_log=%d",
		rn.config.ID, snapIndex, snapTerm, len(rn.persistent.Log))
}

// =========================================================================
// 交易/命令编解码
// =========================================================================

// TxType 交易类型
type TxType string

const (
	TxTypeKV   TxType = "KV"   // KV 操作（兼容原有）
	TxTypeTask TxType = "TASK" // 任务状态变更
)

// Transaction 交易：Raft 日志中的 Command 载体
// 类比区块链：每条 Raft 日志 = 一笔交易，共识达成后由状态机执行
type Transaction struct {
	TxType  TxType          `json:"tx_type"`          // 交易类型
	TxID    string          `json:"tx_id,omitempty"`   // 交易 ID（可选，用于去重/追踪）
	Payload json.RawMessage `json:"payload"`           // 交易载荷（不同类型有不同结构）
}

// Command 表示 KV 操作的载荷（兼容原有）
type Command struct {
	Op    string `json:"op"`    // SET, DELETE
	Key   string `json:"key"`
	Value string `json:"value"`
}

// TaskTransaction 表示任务状态变更的交易载荷
type TaskTransaction struct {
	TaskID  string `json:"task_id"`
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

// EncodeTx 将交易编码为字节（写入 Raft 日志）
func EncodeTx(tx Transaction) ([]byte, error) {
	return json.Marshal(tx)
}

// DecodeTx 将字节解码为交易
func DecodeTx(data []byte) (Transaction, error) {
	var tx Transaction
	err := json.Unmarshal(data, &tx)
	return tx, err
}

// EncodeCommand 将 KV 命令编码为字节（兼容原有调用）
func EncodeCommand(cmd Command) ([]byte, error) {
	payload, err := json.Marshal(cmd)
	if err != nil {
		return nil, err
	}
	tx := Transaction{
		TxType:  TxTypeKV,
		Payload: payload,
	}
	return json.Marshal(tx)
}

// DecodeCommand 将字节解码为 KV 命令（兼容原有调用）
func DecodeCommand(data []byte) (Command, error) {
	// 先尝试作为 Transaction 解析
	var tx Transaction
	if err := json.Unmarshal(data, &tx); err == nil && tx.TxType == TxTypeKV {
		var cmd Command
		err := json.Unmarshal(tx.Payload, &cmd)
		return cmd, err
	}
	// 兼容旧格式：直接作为 Command 解析
	var cmd Command
	err := json.Unmarshal(data, &cmd)
	return cmd, err
}
