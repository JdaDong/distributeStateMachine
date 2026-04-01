#!/bin/bash
#
# 启动 3 节点 Raft 集群，其中一个节点以 Debug 模式启动（使用 Delve）
#
# 用法:
#   ./scripts/start_debug.sh [node_id] [dlv_port]
#
# 参数:
#   node_id   - 要调试的节点 ID (node1/node2/node3)，默认: node1
#   dlv_port  - Delve 调试器监听端口，默认: 2345
#
# 示例:
#   ./scripts/start_debug.sh              # 调试 node1，dlv 监听 :2345
#   ./scripts/start_debug.sh node2        # 调试 node2，dlv 监听 :2345
#   ./scripts/start_debug.sh node3 2346   # 调试 node3，dlv 监听 :2346
#
# IDE 连接:
#   GoLand / VS Code 配置 "Go: Connect to server" → localhost:2345
#

set -e

# ================= 参数 =================
DEBUG_NODE="${1:-node1}"
DLV_PORT="${2:-2345}"

PEERS="node1=localhost:9001,node2=localhost:9002,node3=localhost:9003"

# 节点配置 (节点ID -> Raft端口)
declare -A NODE_PORTS
NODE_PORTS[node1]=9001
NODE_PORTS[node2]=9002
NODE_PORTS[node3]=9003

# ================= 校验 =================
if [[ -z "${NODE_PORTS[$DEBUG_NODE]}" ]]; then
    echo "❌ 无效的节点 ID: $DEBUG_NODE"
    echo "   可选值: node1, node2, node3"
    exit 1
fi

# 检查 dlv 是否安装
if ! command -v dlv &> /dev/null; then
    echo "❌ 未找到 Delve 调试器 (dlv)"
    echo "   请先安装: go install github.com/go-delve/delve/cmd/dlv@latest"
    exit 1
fi

# ================= 准备 =================
echo "============================================"
echo "  Raft 集群 Debug 模式"
echo "============================================"
echo ""
echo "🐛 调试节点: $DEBUG_NODE (Raft=:${NODE_PORTS[$DEBUG_NODE]}, API=:$((NODE_PORTS[$DEBUG_NODE] + 1000)))"
echo "🔌 Delve 端口: :$DLV_PORT"
echo ""
echo "节点信息:"
for node in node1 node2 node3; do
    port=${NODE_PORTS[$node]}
    if [[ "$node" == "$DEBUG_NODE" ]]; then
        echo "  $node: Raft=:$port, API=:$((port + 1000))  ← 🐛 DEBUG"
    else
        echo "  $node: Raft=:$port, API=:$((port + 1000))"
    fi
done
echo ""

# 清理旧数据
rm -rf data/
mkdir -p data/node1 data/node2 data/node3

# 编译（debug 节点需要带调试信息编译）
echo "正在编译..."
# 普通节点 - 正常编译
go build -o bin/raft-node ./cmd/node
# Debug 节点 - 禁用优化，保留调试符号
go build -gcflags="all=-N -l" -o bin/raft-node-debug ./cmd/node
go build -o bin/raft-client ./cmd/client
echo "编译完成！"
echo ""

# ================= 收集 PID =================
PIDS=()

cleanup() {
    echo ""
    echo "正在停止集群..."
    for pid in "${PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    for pid in "${PIDS[@]}"; do
        wait "$pid" 2>/dev/null || true
    done
    echo "集群已停止"
}

trap cleanup EXIT INT TERM

# ================= 启动普通节点 =================
for node in node1 node2 node3; do
    port=${NODE_PORTS[$node]}

    if [[ "$node" == "$DEBUG_NODE" ]]; then
        # 跳过 debug 节点，最后启动
        continue
    fi

    echo "▶ 启动 $node (正常模式)..."
    ./bin/raft-node \
        -id "$node" \
        -listen ":$port" \
        -peers "$PEERS" \
        -data "data/$node" &
    PIDS+=($!)
done

# ================= 启动 Debug 节点 =================
DEBUG_PORT=${NODE_PORTS[$DEBUG_NODE]}
echo ""
echo "🐛 启动 $DEBUG_NODE (Debug 模式)..."
echo "   Delve 将在 :$DLV_PORT 上监听，等待 IDE 连接..."
echo ""

dlv exec ./bin/raft-node-debug \
    --headless \
    --listen=":$DLV_PORT" \
    --api-version=2 \
    --accept-multiclient \
    --continue \
    -- \
    -id "$DEBUG_NODE" \
    -listen ":$DEBUG_PORT" \
    -peers "$PEERS" \
    -data "data/$DEBUG_NODE" &
PIDS+=($!)

echo ""
echo "等待节点启动 (3秒)..."
sleep 3

echo ""
echo "============================================"
echo "  集群已就绪！"
echo "============================================"
echo ""
echo "🐛 Debug 连接信息:"
echo "   协议: DAP / Delve API v2"
echo "   地址: localhost:$DLV_PORT"
echo ""
echo "   VS Code launch.json 配置:"
echo '   {'
echo '     "name": "Attach to Raft Node",'
echo '     "type": "go",'
echo '     "request": "attach",'
echo '     "mode": "remote",'
echo '     "remotePath": "",'
echo '     "port": '$DLV_PORT','
echo '     "host": "127.0.0.1"'
echo '   }'
echo ""
echo "   GoLand: Run → Edit Configurations → + → Go Remote → Host=localhost, Port=$DLV_PORT"
echo ""
echo "客户端操作:"
echo "  KV 操作:"
echo "  ./bin/raft-client -addr localhost:$((DEBUG_PORT + 1000)) set name Alice"
echo "  ./bin/raft-client -addr localhost:$((DEBUG_PORT + 1000)) get name"
echo "  ./bin/raft-client -addr localhost:$((DEBUG_PORT + 1000)) delete name"
echo "  ./bin/raft-client -addr localhost:10001 status"
echo ""
echo "  Task 操作 (完整生命周期):"
echo "  ./bin/raft-client -addr localhost:$((DEBUG_PORT + 1000)) settaskstatus job-001 PENDING"
echo "  ./bin/raft-client -addr localhost:$((DEBUG_PORT + 1000)) settaskstatus job-001 RUNNING \"开始处理\""
echo "  ./bin/raft-client -addr localhost:$((DEBUG_PORT + 1000)) settaskstatus job-001 SUCCESS \"处理完成\""
echo "  ./bin/raft-client -addr localhost:$((DEBUG_PORT + 1000)) gettask job-001"
echo "  ./bin/raft-client -addr localhost:$((DEBUG_PORT + 1000)) listtasks"
echo ""
echo "  运行单元测试:"
echo "  go test ./... -v                    # 运行全部测试"
echo "  go test ./raft/ -v -run TestEncode  # 运行编解码测试"
echo "  go test ./statemachine/ -v          # 运行状态机测试"
echo "  go test ./service/ -v               # 运行服务层测试"
echo ""
echo "按 Ctrl+C 停止集群"

# 等待任意子进程退出
wait
