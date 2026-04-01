package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/distributeStateMachine/raft"
	"github.com/distributeStateMachine/server"
)

func main() {
	// 命令行参数
	id := flag.String("id", "", "节点 ID (如: node1)")
	listenAddr := flag.String("listen", "", "Raft 监听地址 (如: :9001)")
	peers := flag.String("peers", "", "集群节点列表 (格式: id1=addr1,id2=addr2,...)")
	dataDir := flag.String("data", "", "数据存储目录")
	electionTimeout := flag.Int("election-timeout", 300, "选举超时时间(ms)")
	heartbeatInterval := flag.Int("heartbeat-interval", 100, "心跳间隔(ms)")

	flag.Parse()

	if *id == "" || *listenAddr == "" || *peers == "" {
		fmt.Println("用法:")
		fmt.Println("  raft-node -id node1 -listen :9001 -peers node1=localhost:9001,node2=localhost:9002,node3=localhost:9003")
		fmt.Println()
		fmt.Println("参数:")
		flag.PrintDefaults()
		os.Exit(1)
	}

	// 解析 peers
	peerMap := make(map[string]string)
	for _, p := range strings.Split(*peers, ",") {
		parts := strings.SplitN(strings.TrimSpace(p), "=", 2)
		if len(parts) == 2 {
			peerMap[parts[0]] = parts[1]
		}
	}

	if len(peerMap) < 3 {
		log.Fatal("至少需要 3 个节点组成集群")
	}

	// 数据目录
	if *dataDir == "" {
		*dataDir = fmt.Sprintf("data/%s", *id)
	}

	// 创建配置
	config := raft.Config{
		ID:                *id,
		Peers:             peerMap,
		ElectionTimeout:   time.Duration(*electionTimeout) * time.Millisecond,
		HeartbeatInterval: time.Duration(*heartbeatInterval) * time.Millisecond,
		ListenAddr:        *listenAddr,
	}

	// 创建并启动服务器
	srv, err := server.NewServer(config, *dataDir)
	if err != nil {
		log.Fatalf("创建服务器失败: %v", err)
	}

	if err := srv.Start(); err != nil {
		log.Fatalf("启动服务器失败: %v", err)
	}

	// 打印启动信息
	_, port, _ := parsePort(*listenAddr)
	fmt.Printf("============================================\n")
	fmt.Printf("  Raft 节点 [%s] 已启动\n", *id)
	fmt.Printf("  Raft 地址: %s\n", *listenAddr)
	fmt.Printf("  API  地址: :%d\n", port+1000)
	fmt.Printf("  数据目录:  %s\n", *dataDir)
	fmt.Printf("  集群节点:  %v\n", peerMap)
	fmt.Printf("============================================\n")

	// 等待退出信号
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Printf("收到退出信号，正在关闭...")
	srv.Stop()
	log.Printf("节点已关闭")
}

func parsePort(addr string) (string, int, error) {
	var port int
	_, err := fmt.Sscanf(addr, ":%d", &port)
	return "", port, err
}
