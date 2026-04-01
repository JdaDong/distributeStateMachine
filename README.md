# Raft 分布式状态机

基于 **Raft 共识算法** 实现的分布式状态机。纯 Go 实现，零外部依赖。  
采用**区块链交易模型**设计——业务操作封装为 Transaction，经 Raft 共识后由状态机统一执行，保证所有节点状态一致。

## 架构概览

```
┌─────────────────────────────────────────────────────────────────┐
│                          客户端 CLI                              │
│   KV: SET / GET / DELETE / STATUS                               │
│   Task: SETTASKSTATUS / GETTASK / LISTTASKS                     │
└────────────────────────────┬────────────────────────────────────┘
                             │ HTTP JSON API
                ┌────────────┼────────────┐
                ▼            ▼            ▼
          ┌──────────┐ ┌──────────┐ ┌──────────┐
          │  Node 1  │ │  Node 2  │ │  Node 3  │
          │ (Leader) │ │(Follower)│ │(Follower)│
          │          │ │          │ │          │
          │ ┌──────┐ │ │ ┌──────┐ │ │ ┌──────┐ │
          │ │ Task │ │ │ │ Task │ │ │ │ Task │ │
          │ │ Svc  │ │ │ │ Svc  │ │ │ │ Svc  │ │  ← 交易构建层
          │ └──┬───┘ │ │ └──┬───┘ │ │ └──┬───┘ │
          │    │     │ │    │     │ │    │     │
          │    ▼     │ │    ▼     │ │    ▼     │
          │ Propose  │ │ Propose  │ │ Propose  │  ← 提交交易
          │    │     │ │    │     │ │    │     │
          │    ▼     │ │    ▼     │ │    ▼     │
          │ ┌──────┐ │ │ ┌──────┐ │ │ ┌──────┐ │
          │ │ Raft │◄├─┼─┤ Raft │◄├─┼─┤ Raft │ │  ← 共识层（日志复制）
          │ │Engine│ │ │ │Engine│ │ │ │Engine│ │
          │ └──┬───┘ │ │ └──┬───┘ │ │ └──┬───┘ │
          │    ▼     │ │    ▼     │ │    ▼     │
          │ ┌──────┐ │ │ ┌──────┐ │ │ ┌──────┐ │
          │ │State │ │ │ │State │ │ │ │State │ │  ← 状态机（执行交易）
          │ │Machine│ │ │ │Machine│ │ │ │Machine│ │
          │ └──────┘ │ │ └──────┘ │ │ └──────┘ │
          └──────────┘ └──────────┘ └──────────┘
```

## 核心设计：交易模型

### 设计理念

本项目借鉴区块链的交易执行模型，将业务操作封装为 **Transaction（交易）**，通过 Raft 共识保证所有节点以相同顺序执行相同的交易，从而达到状态一致。

```
Client → Server → TaskService（构建交易）→ node.Propose(txBytes)
                                                    │
                                          Raft 日志复制 / 共识
                                                    │
                                          StateMachine.Apply(txBytes)
                                                    │
                                          解释交易 → 校验 → 执行状态变更
```

### 与区块链的概念对应

| 区块链概念 | 本项目 Raft 实现 |
|-----------|-----------------|
| 交易（Transaction） | `raft.Transaction`，编码后写入 Raft 日志 |
| 交易提交（Submit TX） | `node.Propose(txBytes)` |
| 区块共识（Mining / BFT） | Raft 日志复制到多数节点 |
| 世界状态（World State） | `KVStore.data`（KV 数据）+ `KVStore.tasks`（任务状态） |
| 智能合约执行 | `KVStore.Apply()` → `applyTaskTx()` / `applyKVTx()` |
| 交易 Revert | Apply 内校验失败，返回 `{success: false, error: "..."}` |

### 为什么状态校验必须在 Apply 里做？

```
时间线:
  T1: Leader 收到请求 A，读状态 = PENDING，想转为 RUNNING
  T2: Leader 同时收到请求 B，也读到 PENDING，也想转为 RUNNING
  T3: 两笔交易都 Propose 进入 Raft 日志
  T4: 共识达成后，Apply 按日志顺序串行执行:
      - TX_A: PENDING → RUNNING ✅ 校验通过，状态变更
      - TX_B: RUNNING → RUNNING ❌ 校验失败，交易被拒绝（Revert）
```

如果把校验放在 Propose 之前（状态机外部），TX_B 会在 T2 时误认为状态还是 PENDING，导致两个请求都通过。  
**只有 Apply 是串行、确定性执行的**，因此校验逻辑必须放在 Apply 内。

### 交易数据结构

```go
// Transaction 交易：Raft 日志中的 Command 载体
type Transaction struct {
    TxType  TxType          `json:"tx_type"`           // 交易类型: KV / TASK
    TxID    string          `json:"tx_id,omitempty"`    // 交易 ID（可选）
    Payload json.RawMessage `json:"payload"`            // 交易载荷
}

// 交易类型
const (
    TxTypeKV   TxType = "KV"    // KV 操作
    TxTypeTask TxType = "TASK"  // 任务状态变更
)

// KV 交易载荷
type Command struct {
    Op    string `json:"op"`     // SET, DELETE
    Key   string `json:"key"`
    Value string `json:"value"`
}

// Task 交易载荷
type TaskTransaction struct {
    TaskID  string `json:"task_id"`
    Status  string `json:"status"`
    Message string `json:"message,omitempty"`
}
```

### 交易执行流程

```
┌─────────┐    ┌────────────────────────────────────────────────────────┐
│  Client  │    │                    Raft Node 内部                      │
│          │    │                                                        │
│ HTTP Req ├───▶│  Server Handler                                       │
│          │    │       │                                                │
│          │    │       ▼                                                │
│          │    │  TaskService（交易构建层）                               │
│          │    │       │                                                │
│          │    │       │ 1. 参数格式校验（不涉及状态读取）                  │
│          │    │       │ 2. 构建 Transaction{TxType:"TASK", Payload}    │
│          │    │       │ 3. EncodeTx → txBytes                         │
│          │    │       ▼                                                │
│          │    │  node.Propose(txBytes)  ← 本地 Raft 节点              │
│          │    │       │                                                │
│          │    │       │  ← Raft 日志复制到多数节点（共识）              │
│          │    │       ▼                                                │
│          │    │  StateMachine.Apply(txBytes)  ← 共识达成后执行          │
│          │    │       │                                                │
│          │    │       │ 1. DecodeTx → 识别交易类型                     │
│          │    │       │ 2. applyTaskTx():                             │
│          │    │       │    - 校验状态转换合法性 ← 关键，在这里做         │
│          │    │       │    - 合法 → 更新 World State                   │
│          │    │       │    - 非法 → 返回失败（交易 Revert）             │
│          │    │       ▼                                                │
│   Resp ◀├────│  读取最新状态，返回结果                                  │
└─────────┘    └────────────────────────────────────────────────────────┘
```

## 核心特性

- **Raft 共识算法**：完整实现 Leader 选举、日志复制、安全性保证
- **交易模型**：业务操作封装为 Transaction，由状态机统一执行，参考区块链设计
- **分布式状态机**：所有节点维护一致的世界状态（KV 数据 + Task 数据）
- **Proposer 接口解耦**：TaskService 通过 `Proposer` 接口与 Raft 交互，不直接依赖具体实现
- **Apply 内校验**：状态转换的合法性校验在 Apply 内完成，避免 TOCTOU 竞态
- **容错能力**：3 节点可容忍 1 节点故障，多数节点存活即可服务
- **持久化存储**：Raft 状态持久化到磁盘，支持节点重启恢复
- **Debug 支持**：提供单节点 Delve 调试脚本，方便开发排查
- **零依赖**：纯 Go 标准库实现，无需任何第三方依赖

## 项目结构

```
.
├── cmd/
│   ├── node/                  # Raft 节点服务器入口
│   │   └── main.go
│   └── client/                # 客户端 CLI 工具
│       └── main.go
├── raft/
│   ├── raft.go                # Raft 核心协议 + Transaction 编解码
│   ├── state.go               # 状态与数据结构定义
│   └── interfaces.go          # Transport / Storage / StateMachine 接口
├── service/
│   └── task_service.go        # Task 交易构建层 + Proposer 接口
├── statemachine/
│   └── kvstore.go             # 状态机：Apply 执行交易，校验+变更世界状态
├── transport/
│   └── http_transport.go      # HTTP JSON 传输层（Raft RPC）
├── storage/
│   └── file_storage.go        # 文件持久化存储
├── server/
│   └── server.go              # 服务器封装（HTTP API + 组件组装）
├── proto/
│   ├── raft.proto             # Protocol Buffers 定义（参考用）
│   └── types.go               # Go 类型定义
└── scripts/
    ├── start_cluster.sh       # 集群启动脚本
    └── start_debug.sh         # 单节点 Debug 启动脚本
```

## 分层架构

```
┌──────────────────────────────────────────────────────────┐
│                    HTTP API (server.go)                   │  ← 接入层
│   /api/set, /api/get, /api/delete, /api/status           │
│   /api/task/set-status, /api/task/get, /api/task/list    │
├──────────────────────────────────────────────────────────┤
│               TaskService (task_service.go)               │  ← 交易构建层
│   构建 Transaction → Propose 给本地 Raft 节点              │
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

### 关键接口

```go
// Proposer 接口（service/task_service.go）
// TaskService 通过此接口与 Raft 解耦，*raft.RaftNode 天然实现
type Proposer interface {
    Propose(command []byte) (raft.ApplyMsg, error)
    GetLeaderID() string
}

// StateMachine 接口（raft/interfaces.go）
// 状态机的 Apply 是唯一改变状态的入口
type StateMachine interface {
    Apply(command []byte) ([]byte, error)
    Snapshot() ([]byte, error)
    Restore(snapshot []byte) error
}

// Transport 接口（raft/interfaces.go）
// 节点间 Raft RPC 通信
type Transport interface {
    SendRequestVote(ctx, target, req) (*resp, error)
    SendAppendEntries(ctx, target, req) (*resp, error)
    Start() error
    Stop() error
}
```

## Raft 算法实现详解

### 1. Leader 选举

```
[Follower] ──超时──> [Candidate] ──获得多数票──> [Leader]
    ▲                    │                        │
    │                    ▼                        │
    │              发现更高任期                    │
    │                    │                        ▼
    └────────────────────┘                   定期心跳
```

- 所有节点初始为 **Follower**
- 选举超时（随机 300~600ms）后转为 **Candidate**，发起 `RequestVote` RPC
- 获得多数节点投票后成为 **Leader**
- Leader 当选后发送 no-op 日志提交之前任期的日志
- Leader 通过 `AppendEntries` 心跳（100ms 间隔）维持权威

### 2. 日志复制与交易共识

```
Client ──> Leader: SetTaskStatus(job-001, RUNNING)
                 │
                 │  TaskService 构建 Transaction
                 │  node.Propose(txBytes)
                 │
                 ├── AppendEntries(txBytes) ──> Follower1 (ACK)
                 ├── AppendEntries(txBytes) ──> Follower2 (ACK)
                 │
                 ▼
           多数确认 → commitIndex 推进
                 │
                 ▼
           Apply Loop: StateMachine.Apply(txBytes)
                 │
                 ▼
           applyTaskTx: 校验状态转换 → 更新 World State
                 │
                 ▼
           返回结果给 Propose 调用方
```

### 3. 安全性保证

- **选举限制**：候选人的日志必须至少和投票者一样新（比较 lastLogTerm 和 lastLogIndex）
- **提交限制**：只提交当前任期的日志条目
- **日志匹配**：通过 prevLogIndex / prevLogTerm 保证日志一致性
- **串行 Apply**：状态机 Apply 串行执行，保证所有节点以相同顺序执行交易

## Task 状态流转

```
                 ┌──────────┐
                 │ PENDING  │
                 └────┬─────┘
                      │
              ┌───────┴───────┐
              ▼               ▼
        ┌──────────┐   ┌───────────┐
        │ RUNNING  │   │ CANCELLED │
        └────┬─────┘   └───────────┘
             │
     ┌───────┼───────┬───────────┐
     ▼       ▼       ▼           ▼
┌─────────┐ ┌──────┐ ┌──────────┐ ┌─────────┐
│ SUCCESS │ │FAILED│ │CANCELLED │ │ TIMEOUT │
└─────────┘ └──────┘ └──────────┘ └─────────┘
     ↑ 以上四种为终态，不可再变更
```

| 状态 | 说明 | 是否终态 | 可转换到 |
|------|------|----------|---------|
| `PENDING` | 等待执行 | 否 | RUNNING, CANCELLED |
| `RUNNING` | 执行中 | 否 | SUCCESS, FAILED, CANCELLED, TIMEOUT |
| `SUCCESS` | 执行成功 | ✅ 是 | — |
| `FAILED` | 执行失败 | ✅ 是 | — |
| `CANCELLED` | 已取消 | ✅ 是 | — |
| `TIMEOUT` | 执行超时 | ✅ 是 | — |

状态转换校验在状态机 `Apply` 内完成（`applyTaskTx` → `validateTransition`），非法转换将导致交易被拒绝（类似区块链的 Revert）。

## 快速开始

### 编译

```bash
go build -o bin/raft-node ./cmd/node
go build -o bin/raft-client ./cmd/client
```

### 启动 3 节点集群

**方式一：使用脚本**

```bash
chmod +x scripts/start_cluster.sh
./scripts/start_cluster.sh
```

**方式二：手动启动**

```bash
# 终端1 - 启动 node1
./bin/raft-node -id node1 -listen :9001 \
  -peers "node1=localhost:9001,node2=localhost:9002,node3=localhost:9003"

# 终端2 - 启动 node2
./bin/raft-node -id node2 -listen :9002 \
  -peers "node1=localhost:9001,node2=localhost:9002,node3=localhost:9003"

# 终端3 - 启动 node3
./bin/raft-node -id node3 -listen :9003 \
  -peers "node1=localhost:9001,node2=localhost:9002,node3=localhost:9003"
```

### KV 客户端操作

```bash
# 设置键值对（KV 交易，经 Raft 共识）
./bin/raft-client -addr localhost:10001 set name Alice
./bin/raft-client -addr localhost:10001 set age 30

# 读取值（直接读本地状态机，最终一致性）
./bin/raft-client -addr localhost:10001 get name
# ✅ 成功: Alice

# 从其他节点读取（数据已通过 Raft 复制）
./bin/raft-client -addr localhost:10002 get name
# ✅ 成功: Alice

# 删除键
./bin/raft-client -addr localhost:10001 delete age

# 查看节点状态
./bin/raft-client -addr localhost:10001 status
```

### Task 任务管理

```bash
# 创建任务（TASK 交易：PENDING 状态，经 Raft 共识）
./bin/raft-client -addr localhost:10001 settaskstatus job-001 PENDING

# 更新为运行中（TASK 交易：PENDING → RUNNING，状态机内校验转换合法性）
./bin/raft-client -addr localhost:10001 settaskstatus job-001 RUNNING "开始处理数据"

# 标记任务成功（TASK 交易：RUNNING → SUCCESS）
./bin/raft-client -addr localhost:10001 settaskstatus job-001 SUCCESS "处理完成，共 1024 条"

# 尝试非法转换（SUCCESS 是终态，不可再变更 → 交易被 Revert）
./bin/raft-client -addr localhost:10001 settaskstatus job-001 RUNNING "重新执行"
# ❌ 失败: cannot transition from terminal status SUCCESS to RUNNING

# 查询单个任务（读取本地状态机）
./bin/raft-client -addr localhost:10001 gettask job-001
# ✅ 任务 [job-001] 状态: SUCCESS
#    消息: 处理完成，共 1024 条
#    创建: 2026-04-01T15:00:00+08:00
#    更新: 2026-04-01T15:02:30+08:00
#    完成: 2026-04-01T15:02:30+08:00

# 从其他节点查询（数据已通过 Raft 复制）
./bin/raft-client -addr localhost:10002 gettask job-001

# 列出所有任务
./bin/raft-client -addr localhost:10001 listtasks
# 📋 共 3 个任务:
#   ✅ [job-001] SUCCESS - 处理完成
#   🔄 [job-002] RUNNING - 正在处理
#   ⏳ [job-003] PENDING
```

## Debug 模式

提供单节点调试脚本，可选择一个节点以 Delve 调试模式启动，其余正常运行：

```bash
# 调试 node1（默认），Delve 监听 :2345
./scripts/start_debug.sh

# 调试 node2
./scripts/start_debug.sh node2

# 调试 node3，指定 Delve 端口
./scripts/start_debug.sh node3 2346
```

IDE 连接（VS Code `launch.json`）：

```json
{
  "name": "Attach to Raft Node",
  "type": "go",
  "request": "attach",
  "mode": "remote",
  "port": 2345,
  "host": "127.0.0.1"
}
```

### 容错测试

```bash
# 1. 启动集群并写入数据
./bin/raft-client -addr localhost:10001 set key1 value1

# 2. 停止一个 Follower 节点（Ctrl+C 对应终端）
# 集群仍可正常服务（2/3 节点存活）

# 3. 继续写入（交易仍可达成共识）
./bin/raft-client -addr localhost:10001 settaskstatus job-100 RUNNING

# 4. 重启停止的节点，它会自动追赶日志并 Apply 所有交易
```

## API 端口映射

| 节点   | Raft 端口 | Client API 端口 |
|--------|-----------|-----------------|
| node1  | 9001      | 10001           |
| node2  | 9002      | 10002           |
| node3  | 9003      | 10003           |

Raft 端口用于节点间 RPC 通信（`/raft/vote`、`/raft/append`），Client API 端口用于客户端交互。

## HTTP API

### KV 接口

#### SET - 设置键值对

```bash
curl -X POST http://localhost:10001/api/set \
  -H "Content-Type: application/json" \
  -d '{"key":"name","value":"Alice"}'
```

内部流程：构建 `Transaction{TxType: "KV", Payload: {Op: "SET", Key: "name", Value: "Alice"}}` → Propose → 共识 → Apply

#### GET - 获取值

```bash
curl "http://localhost:10001/api/get?key=name"
```

直接读本地状态机，不经过 Raft 共识（最终一致性读取）。

#### DELETE - 删除键

```bash
curl -X POST http://localhost:10001/api/delete \
  -H "Content-Type: application/json" \
  -d '{"key":"name"}'
```

#### STATUS - 节点状态

```bash
curl "http://localhost:10001/api/status"
```

返回节点角色、任期、集群信息、所有 KV/Task 数据。

### Task 接口

#### SetTaskStatus - 设置任务状态

```bash
curl -X POST http://localhost:10001/api/task/set-status \
  -H "Content-Type: application/json" \
  -d '{"task_id":"job-001","status":"PENDING","message":"任务已创建"}'
```

内部流程：
1. TaskService 参数格式校验
2. 构建 `Transaction{TxType: "TASK", Payload: {TaskID, Status, Message}}`
3. `node.Propose(txBytes)` → Raft 日志复制 → 共识
4. `StateMachine.Apply()` → `applyTaskTx()` → 状态转换校验 → 更新 World State
5. 返回执行结果

成功响应：

```json
{
  "success": true,
  "task": {
    "task_id": "job-001",
    "status": "PENDING",
    "message": "任务已创建",
    "created_at": "2026-04-01T15:00:00+08:00",
    "updated_at": "2026-04-01T15:00:00+08:00"
  }
}
```

非法状态转换响应（交易被 Revert）：

```json
{
  "success": false,
  "error": "state transition rejected: current status is SUCCESS"
}
```

非 Leader 响应：

```json
{
  "success": false,
  "error": "not leader, leader_hint: node1"
}
```

#### GetTask - 获取任务详情

```bash
curl "http://localhost:10001/api/task/get?task_id=job-001"
```

直接读本地状态机（最终一致性读取）。

#### ListTasks - 列出所有任务

```bash
curl "http://localhost:10001/api/task/list"
```

## 设计决策

| 决策 | 选择 | 理由 |
|------|------|------|
| 通信协议 | HTTP JSON | 零依赖、便于调试、开发友好 |
| 持久化 | JSON 文件 | 简单可靠、原子写入（先写临时文件再 rename） |
| 状态机 | 内存 KV Store | 示例用途、性能好、支持快照恢复 |
| 序列化 | JSON | 可读性强、标准库支持 |
| 交易模型 | Transaction 封装 | 参考区块链，统一不同类型操作的提交和执行 |
| 状态校验 | Apply 内校验 | 避免 TOCTOU 竞态，保证所有节点一致性 |
| 业务解耦 | Proposer 接口 | TaskService 不直接依赖 RaftNode，便于测试和替换 |
| 读取方式 | 本地状态机读取 | 最终一致性，无需共识开销；如需强一致读可扩展 ReadIndex |

## 可扩展方向

- **新交易类型**：在 `raft.TxType` 中添加新类型，在 `KVStore.Apply()` 中增加对应的处理分支
- **强一致性读**：实现 ReadIndex 或 LeaseRead，保证读取最新已提交数据
- **日志压缩**：实现 Snapshot + Log Compaction，防止日志无限增长
- **动态成员变更**：支持集群节点的动态加入/移除
- **交易去重**：利用 `Transaction.TxID` 实现幂等性

## License

MIT
# distributeStateMachine
