# 分布式状态机 — 架构演进总结

## 📌 阶段一：基础 Raft + KV 存储

### 目标

实现一个最小可用的分布式 KV 存储。

### 架构图

```
┌──────────────┐
│  Client CLI  │
│  SET/GET/DEL │
└──────┬───────┘
       │ HTTP JSON
       ▼
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│    Node 1    │◄───▶│    Node 2    │◄───▶│    Node 3    │
│              │     │              │     │              │
│ ┌──────────┐ │     │ ┌──────────┐ │     │ ┌──────────┐ │
│ │Raft Core │ │     │ │Raft Core │ │     │ │Raft Core │ │
│ └────┬─────┘ │     │ └────┬─────┘ │     │ └────┬─────┘ │
│      ▼       │     │      ▼       │     │      ▼       │
│ ┌──────────┐ │     │ ┌──────────┐ │     │ ┌──────────┐ │
│ │ KV Store │ │     │ │ KV Store │ │     │ │ KV Store │ │
│ │ (内存)   │ │     │ │ (内存)   │ │     │ │ (内存)   │ │
│ └──────────┘ │     │ └──────────┘ │     │ └──────────┘ │
└──────────────┘     └──────────────┘     └──────────────┘
```

### 核心模块

| 模块 | 文件 | 职责 |
|------|------|------|
| Raft 核心 | `raft/raft.go` + `state.go` | Leader 选举、日志复制、Apply Loop |
| 接口定义 | `raft/interfaces.go` | Transport / Storage / StateMachine 三大接口 |
| 状态机 | `statemachine/kvstore.go` | 内存 KV 存储，Apply 执行 SET/DELETE |
| 传输层 | `transport/http_transport.go` | HTTP JSON 实现 Raft RPC（`/raft/vote`、`/raft/append`） |
| 持久化 | `storage/file_storage.go` | JSON 文件原子写入 |
| 服务端 | `server/server.go` | 组装组件 + HTTP API |
| 协议类型 | `proto/types.go` | 手写 Go 类型（兼容 protobuf 风格） |

### 设计特点

- **三层接口解耦**：`Transport`、`Storage`、`StateMachine` 均为 interface，可替换实现
- **命令直接编码**：`Command{Op, Key, Value}` 直接 JSON 序列化写入 Raft 日志
- **Server 内直接 Propose**：HTTP Handler 直接调用 `node.Propose(cmdBytes)`
- **最终一致性读**：GET 直接读本地状态机，不经共识

---

## 📌 阶段二：引入交易模型（Transaction Model）

### 目标

支持多种业务类型（KV + Task），借鉴区块链交易模型统一操作封装。

### 命令编码方式的演进

```
阶段一:  Command{SET, key, val}  ──直接──▶  Raft Log

阶段二:  Transaction{
           TxType: "KV" / "TASK",     ──统一封装──▶  Raft Log
           Payload: {...}
         }
```

### 新增的核心抽象

定义在 `raft/raft.go` 底部：

```go
type Transaction struct {
    TxType  TxType          // "KV" 或 "TASK"
    TxID    string          // 可选，用于去重/追踪
    Payload json.RawMessage // 不同类型的载荷
}

// KV 载荷（兼容阶段一）
type Command struct { Op, Key, Value string }

// Task 载荷（新增）
type TaskTransaction struct { TaskID, Status, Message string }
```

### 状态机 Apply 升级

```
Apply(command []byte)
  │
  ├── DecodeTx → 识别 TxType
  │
  ├── TxTypeKV   → applyKVTx()     → executeKVOp()
  ├── TxTypeTask → applyTaskTx()   → validateTransition() → 更新 World State
  └── 未知类型   → 兼容旧格式 applyLegacyCommand()
```

### 区块链类比对应表

| 区块链概念 | Raft 实现 |
|-----------|----------|
| 交易（Transaction） | `raft.Transaction`，编码后写入日志 |
| 交易提交（Submit TX） | `node.Propose(txBytes)` |
| 区块共识（Mining） | Raft 日志复制到多数节点 |
| 世界状态（World State） | `KVStore.data` + `KVStore.tasks` |
| 智能合约执行 | `KVStore.Apply()` 内的分派逻辑 |
| 交易 Revert | Apply 内校验失败，返回 `{success: false}` |

### 关键设计决策——Apply 内校验

```
为什么状态校验必须在 Apply 里做？

T1: Leader 收到请求 A: PENDING → RUNNING
T2: Leader 同时收到请求 B: PENDING → RUNNING（并发读到同一状态）
T3: 两笔交易都 Propose 进日志
T4: Apply 按日志顺序串行执行:
    TX_A: PENDING → RUNNING ✅
    TX_B: RUNNING → RUNNING ❌ （被拒绝）

结论: 只有 Apply 是串行、确定性的，校验必须在这里做
```

---

## 📌 阶段三：引入服务层 + Proposer 接口解耦

### 目标

业务逻辑与 Raft 解耦，支持独立测试和扩展。

### 分层架构

```
┌──────────────────────────────────────────────────────────┐
│                    HTTP API (server.go)                   │  ← 接入层
│   /api/set, /api/get, /api/delete, /api/status           │
│   /api/task/set-status, /api/task/get, /api/task/list    │
├──────────────────────────────────────────────────────────┤
│               TaskService (task_service.go)               │  ← 交易构建层
│   构建 Transaction → 通过 Proposer 接口提交               │
│   通过 Proposer 接口解耦，不直接依赖 RaftNode              │
├──────────────────────────────────────────────────────────┤
│                 Raft Engine (raft.go)                     │  ← 共识层
│   Leader 选举、日志复制、Propose、Apply Loop              │
│   Transport: HTTP JSON RPC (vote / append)               │
├──────────────────────────────────────────────────────────┤
│              StateMachine (kvstore.go)                    │  ← 执行层
│   Apply: 解析 Transaction → 校验 → 执行状态变更            │
│   World State: KV data + Task data                       │
├──────────────────────────────────────────────────────────┤
│               Storage (file_storage.go)                  │  ← 持久层
│   Raft 状态 JSON 文件持久化，原子写入                      │
└──────────────────────────────────────────────────────────┘
```

### 新增 Proposer 接口

定义在 `service/task_service.go`：

```go
type Proposer interface {
    Propose(command []byte) (raft.ApplyMsg, error)
    GetLeaderID() string
}
```

- `*raft.RaftNode` 天然实现此接口
- `TaskService` 只依赖 `Proposer`，不依赖具体的 `RaftNode`
- 测试时可以 mock Proposer，无需启动真实 Raft 集群

### 职责分离

| 层 | 职责 | 不做什么 |
|----|------|---------|
| **Server** (接入层) | HTTP 路由、请求解码、响应编码 | 不构建交易、不调 Propose |
| **TaskService** (交易构建层) | 参数格式校验、构建 Transaction、调用 Propose | 不做状态校验（留给 Apply） |
| **Raft Engine** (共识层) | 日志复制、投票、commitIndex 推进 | 不理解业务语义 |
| **StateMachine** (执行层) | 解析交易、校验状态转换、变更 World State | 不知道网络和共识细节 |
| **Storage** (持久层) | 原子写入 Raft 状态 | 不理解日志内容 |

### 完整请求链路（以 SetTaskStatus 为例）

```
Client HTTP POST /api/task/set-status
  │
  ▼
Server.handleSetTaskStatus()          ← 接入层：解码 JSON
  │
  ▼
TaskService.SetTaskStatus()           ← 交易构建层
  │ 1. 参数格式校验（不读状态）
  │ 2. 构建 Transaction{TxType:"TASK", Payload:{TaskID, Status}}
  │ 3. EncodeTx → txBytes
  │
  ▼
node.Propose(txBytes)                 ← 共识层
  │ 1. 追加到本地日志
  │ 2. AppendEntries 复制到 Follower
  │ 3. 多数确认 → commitIndex 推进
  │
  ▼
StateMachine.Apply(txBytes)           ← 执行层（所有节点一致执行）
  │ 1. DecodeTx → 识别 TxTypeTask
  │ 2. applyTaskTx():
  │    - validateTransition(from, to) ← 关键校验点
  │    - 合法 → 更新 World State
  │    - 非法 → 返回失败（Revert）
  │
  ▼
TaskService 读取最新状态，返回响应     ← 回到交易构建层
  │
  ▼
Server 编码 JSON 返回 Client          ← 回到接入层
```

---

## 三阶段对比总结

| 维度 | 阶段一 | 阶段二 | 阶段三 |
|------|--------|--------|--------|
| **命令格式** | 单一 `Command` | 统一 `Transaction` 封装 | 同阶段二 |
| **业务类型** | 仅 KV | KV + Task | 同阶段二，但分层更清晰 |
| **状态校验** | 无（KV 无需校验） | Apply 内校验 | 同阶段二 |
| **Server 职责** | 直接 Propose | 直接 Propose | 委托 TaskService |
| **业务解耦** | 无 | 部分（Transaction 抽象） | 完全（Proposer 接口） |
| **可测试性** | 需要启动真实集群 | 需要启动真实集群 | 可 mock Proposer |
| **扩展新业务** | 改 Server + StateMachine | 加 TxType + Apply 分支 | 加 Service + TxType + Apply 分支 |
| **类比** | 简单数据库 | 区块链节点 | 分层微服务 + 区块链 |

---

## 🔮 可扩展方向（潜在的阶段四）

| 方向 | 说明 |
|------|------|
| **新交易类型** | 添加 `TxType`，在 Apply 增加处理分支，创建对应 Service |
| **强一致性读** | 实现 ReadIndex / LeaseRead，保证读到最新已提交数据 |
| **动态成员变更** | 支持节点在线加入/退出集群 |
| **交易去重** | 利用 `Transaction.TxID` 实现幂等性 |
| **Apply 结果回传** | 将 Apply 的 `[]byte` 结果直接回传给 Propose 调用方，替代当前"读状态确认"的方式 |

---

## 📌 阶段四：快照落盘与日志压缩

### 目标

解决日志无限膨胀问题。通过状态机快照（Snapshot）+ 日志截断（Log Compaction），实现：
1. 快照包含完整状态机数据，落盘到 `snapshot.json`
2. 旧日志被截断，只保留快照之后的增量日志
3. 重启时先恢复快照，再重放剩余日志，大幅加速恢复

### 问题背景

```
阶段三的持久化策略:
  ┌─────────────────────────────────────────┐
  │ raft_state.json                         │
  │ ┌─────────────────────────────────────┐ │
  │ │ CurrentTerm, VotedFor               │ │
  │ │ Log: [entry1, entry2, ... entryN]   │ │  ← 全量日志，N 无限增长
  │ └─────────────────────────────────────┘ │
  └─────────────────────────────────────────┘

问题:
  1. 文件越来越大（每次 SaveState 都全量序列化）
  2. 重启要从头重放所有日志到状态机
  3. 网络同步新节点要发送完整日志
```

### 核心设计

```
阶段四的持久化策略:
  ┌─────────────────────────────────────────┐
  │ snapshot.json                           │  ← 新增：状态机快照
  │ ┌─────────────────────────────────────┐ │
  │ │ Metadata: {index=100, term=3}       │ │  ← 快照截断点
  │ │ Data: {KV数据 + Task数据}            │ │  ← 完整世界状态
  │ └─────────────────────────────────────┘ │
  └─────────────────────────────────────────┘

  ┌─────────────────────────────────────────┐
  │ raft_state.json                         │
  │ ┌─────────────────────────────────────┐ │
  │ │ CurrentTerm, VotedFor               │ │
  │ │ SnapshotIndex=100, SnapshotTerm=3   │ │  ← 新增字段
  │ │ Log: [entry101, entry102, ...]      │ │  ← 只保留增量日志
  │ └─────────────────────────────────────┘ │
  └─────────────────────────────────────────┘
```

### 接口扩展

`Storage` 接口新增两个方法：

```go
type Storage interface {
    SaveState(state *PersistentState) error
    LoadState() (*PersistentState, error)
    SaveSnapshot(metadata SnapshotMetadata, data []byte) error   // 新增
    LoadSnapshot() (*SnapshotMetadata, []byte, error)            // 新增
    Close() error
}

type SnapshotMetadata struct {
    LastIncludedIndex uint64  // 快照覆盖的最后一条日志索引
    LastIncludedTerm  uint64  // 快照覆盖的最后一条日志任期
    CreatedAt         string  // 创建时间
}
```

### 逻辑索引 vs 物理索引

日志截断后，逻辑索引（全局递增）和物理数组下标不再对应：

```
截断前: Log = [entry1, entry2, ..., entry100, entry101, entry102]
                 ↑物理0                        ↑物理100

截断后 (SnapshotIndex=100):
        Log = [entry101, entry102]
                ↑物理0    ↑物理1

转换公式:
  物理下标 = 逻辑索引 - SnapshotIndex - 1
  逻辑索引 = 物理下标 + SnapshotIndex + 1
```

关键辅助方法：

```go
func (rn *RaftNode) logicalToPhysical(logicalIndex uint64) int
func (rn *RaftNode) physicalToLogical(physicalIndex int) uint64
func (rn *RaftNode) getLogEntry(logicalIndex uint64) *LogEntry
func (rn *RaftNode) lastLogIndex() uint64
```

### 快照触发流程

```
applyCommitted()  →  每批日志应用完后
  │
  ▼
maybeSnapshot()   →  检查日志条目数 >= SnapshotThreshold
  │
  ├── 1. StateMachine.Snapshot()     → 序列化完整状态
  ├── 2. Storage.SaveSnapshot()      → 原子写入 snapshot.json
  ├── 3. 截断 persistent.Log         → 只保留 LastApplied 之后的日志
  ├── 4. 更新 SnapshotIndex/Term     → 记录截断点
  └── 5. Storage.SaveState()         → 持久化截断后的 Raft 状态
```

### 重启恢复流程

```
NewRaftNode()
  │
  ├── 步骤 1: Storage.LoadSnapshot()
  │    └── 有快照 → StateMachine.Restore(snapData)
  │
  ├── 步骤 2: Storage.LoadState()
  │    └── 恢复 persistent 状态（含截断后的日志）
  │
  ├── 步骤 3: 设置 LastApplied = SnapshotIndex
  │    └── 快照内的数据已经恢复，不需要重新 Apply
  │
  └── 启动后: applyLoop()
       └── 从 SnapshotIndex+1 开始重放剩余日志
```

### 改动文件清单

| 文件 | 改动 |
|------|------|
| `raft/interfaces.go` | Storage 接口增加 SaveSnapshot/LoadSnapshot；新增 SnapshotMetadata |
| `raft/state.go` | PersistentState 增加 SnapshotIndex/SnapshotTerm；Config 增加 SnapshotThreshold |
| `raft/raft.go` | 新增 logicalToPhysical/getLogEntry 等索引转换方法；新增 maybeSnapshot()；改造 NewRaftNode 恢复流程；改造 applyCommitted/HandleAppendEntries/sendAppendEntries/advanceCommitIndex 适配逻辑索引 |
| `storage/file_storage.go` | 实现 SaveSnapshot/LoadSnapshot，原子写入 snapshot.json |

### 配置参数

```go
Config{
    SnapshotThreshold: 100,  // 日志条目达到 100 时触发快照（默认值）
}
```

### 四阶段对比总结

| 维度 | 阶段一~三 | 阶段四 |
|------|----------|--------|
| **日志存储** | 全量日志写入 raft_state.json | 截断后增量日志 + 独立快照文件 |
| **状态机持久化** | 无（纯内存） | 快照落盘 snapshot.json |
| **重启恢复** | 全量重放所有日志 | 加载快照 + 重放增量日志 |
| **磁盘空间** | 随时间无限增长 | 快照替代旧日志，空间受控 |
| **SaveState 性能** | 日志越多越慢 | 只序列化增量部分 |
