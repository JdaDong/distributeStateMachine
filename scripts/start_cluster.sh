#!/bin/bash
#
# 启动 3 节点 Raft 集群的脚本
#

set -e

PEERS="node1=localhost:9001,node2=localhost:9002,node3=localhost:9003"

echo "============================================"
echo "  启动 Raft 分布式状态机集群 (3 节点)"
echo "============================================"
echo ""
echo "节点信息:"
echo "  node1: Raft=:9001, API=:10001"
echo "  node2: Raft=:9002, API=:10002"
echo "  node3: Raft=:9003, API=:10003"
echo ""

# 清理旧数据
rm -rf data/
mkdir -p data/node1 data/node2 data/node3

# 编译
echo "正在编译..."
go build -o bin/raft-node ./cmd/node
go build -o bin/raft-client ./cmd/client
echo "编译完成！"
echo ""

# 启动节点
echo "启动 node1..."
./bin/raft-node -id node1 -listen :9001 -peers "$PEERS" -data data/node1 &
PID1=$!

echo "启动 node2..."
./bin/raft-node -id node2 -listen :9002 -peers "$PEERS" -data data/node2 &
PID2=$!

echo "启动 node3..."
./bin/raft-node -id node3 -listen :9003 -peers "$PEERS" -data data/node3 &
PID3=$!

echo ""
echo "所有节点已启动！PID: $PID1, $PID2, $PID3"
echo ""
echo "等待选举完成 (3秒)..."
sleep 3
echo ""

echo "============================================"
echo "  集群已就绪！"
echo "============================================"
echo ""
echo "使用客户端操作:"
echo "  ./bin/raft-client -addr localhost:10001 set name Alice"
echo "  ./bin/raft-client -addr localhost:10001 get name"
echo "  ./bin/raft-client -addr localhost:10002 status"
echo ""
echo "按 Ctrl+C 停止集群"

# 捕获退出信号并清理
cleanup() {
    echo ""
    echo "正在停止集群..."
    kill $PID1 $PID2 $PID3 2>/dev/null || true
    wait $PID1 $PID2 $PID3 2>/dev/null || true
    echo "集群已停止"
}

trap cleanup EXIT INT TERM

# 等待任意子进程退出
wait
