package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

func main() {
	addr := flag.String("addr", "localhost:10001", "节点 API 地址")
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		printUsage()
		os.Exit(1)
	}

	client := &http.Client{Timeout: 10 * time.Second}

	switch strings.ToUpper(args[0]) {
	case "SET":
		if len(args) < 3 {
			fmt.Println("用法: client set <key> <value>")
			os.Exit(1)
		}
		doSet(client, *addr, args[1], args[2])
	case "GET":
		if len(args) < 2 {
			fmt.Println("用法: client get <key>")
			os.Exit(1)
		}
		doGet(client, *addr, args[1])
	case "DELETE":
		if len(args) < 2 {
			fmt.Println("用法: client delete <key>")
			os.Exit(1)
		}
		doDelete(client, *addr, args[1])
	case "STATUS":
		doStatus(client, *addr)

	// ============ Task 命令 ============
	case "SETTASKSTATUS":
		if len(args) < 3 {
			fmt.Println("用法: client settaskstatus <task_id> <status> [message]")
			fmt.Println("状态: PENDING, RUNNING, SUCCESS, FAILED, CANCELLED, TIMEOUT")
			os.Exit(1)
		}
		msg := ""
		if len(args) >= 4 {
			msg = strings.Join(args[3:], " ")
		}
		doSetTaskStatus(client, *addr, args[1], args[2], msg)
	case "GETTASK":
		if len(args) < 2 {
			fmt.Println("用法: client gettask <task_id>")
			os.Exit(1)
		}
		doGetTask(client, *addr, args[1])
	case "LISTTASKS":
		doListTasks(client, *addr)

	default:
		fmt.Printf("未知命令: %s\n", args[0])
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("Raft 分布式状态机客户端")
	fmt.Println()
	fmt.Println("用法:")
	fmt.Println("  client -addr <节点API地址> <命令> [参数...]")
	fmt.Println()
	fmt.Println("KV 命令:")
	fmt.Println("  set <key> <value>                          设置键值对")
	fmt.Println("  get <key>                                  获取值")
	fmt.Println("  delete <key>                               删除键")
	fmt.Println("  status                                     查看节点状态")
	fmt.Println()
	fmt.Println("Task 命令:")
	fmt.Println("  settaskstatus <task_id> <status> [message] 设置任务状态")
	fmt.Println("  gettask <task_id>                          获取任务详情")
	fmt.Println("  listtasks                                  列出所有任务")
	fmt.Println()
	fmt.Println("Task 状态: PENDING, RUNNING, SUCCESS, FAILED, CANCELLED, TIMEOUT")
	fmt.Println()
	fmt.Println("示例:")
	fmt.Println("  client -addr localhost:10001 set name Alice")
	fmt.Println("  client -addr localhost:10001 get name")
	fmt.Println("  client -addr localhost:10001 settaskstatus job-001 PENDING")
	fmt.Println("  client -addr localhost:10001 settaskstatus job-001 RUNNING \"开始处理\"")
	fmt.Println("  client -addr localhost:10001 settaskstatus job-001 SUCCESS \"处理完成\"")
	fmt.Println("  client -addr localhost:10001 gettask job-001")
	fmt.Println("  client -addr localhost:10001 listtasks")
}

// =========================================================================
// KV 操作
// =========================================================================

func doSet(client *http.Client, addr, key, value string) {
	body := map[string]string{
		"key":   key,
		"value": value,
	}
	data, _ := json.Marshal(body)

	resp, err := client.Post(
		fmt.Sprintf("http://%s/api/set", addr),
		"application/json",
		bytes.NewReader(data),
	)
	if err != nil {
		fmt.Printf("❌ 请求失败: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	printResponse(resp)
}

func doGet(client *http.Client, addr, key string) {
	resp, err := client.Get(fmt.Sprintf("http://%s/api/get?key=%s", addr, key))
	if err != nil {
		fmt.Printf("❌ 请求失败: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	printResponse(resp)
}

func doDelete(client *http.Client, addr, key string) {
	body := map[string]string{
		"key": key,
	}
	data, _ := json.Marshal(body)

	resp, err := client.Post(
		fmt.Sprintf("http://%s/api/delete", addr),
		"application/json",
		bytes.NewReader(data),
	)
	if err != nil {
		fmt.Printf("❌ 请求失败: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	printResponse(resp)
}

func doStatus(client *http.Client, addr string) {
	resp, err := client.Get(fmt.Sprintf("http://%s/api/status", addr))
	if err != nil {
		fmt.Printf("❌ 请求失败: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var result map[string]interface{}
	json.Unmarshal(body, &result)

	prettyJSON, _ := json.MarshalIndent(result, "", "  ")
	fmt.Println(string(prettyJSON))
}

// =========================================================================
// Task 操作
// =========================================================================

func doSetTaskStatus(client *http.Client, addr, taskID, status, message string) {
	body := map[string]string{
		"task_id": taskID,
		"status":  strings.ToUpper(status),
	}
	if message != "" {
		body["message"] = message
	}
	data, _ := json.Marshal(body)

	resp, err := client.Post(
		fmt.Sprintf("http://%s/api/task/set-status", addr),
		"application/json",
		bytes.NewReader(data),
	)
	if err != nil {
		fmt.Printf("❌ 请求失败: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	printTaskResponse(resp)
}

func doGetTask(client *http.Client, addr, taskID string) {
	resp, err := client.Get(fmt.Sprintf("http://%s/api/task/get?task_id=%s", addr, taskID))
	if err != nil {
		fmt.Printf("❌ 请求失败: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	printTaskResponse(resp)
}

func doListTasks(client *http.Client, addr string) {
	resp, err := client.Get(fmt.Sprintf("http://%s/api/task/list", addr))
	if err != nil {
		fmt.Printf("❌ 请求失败: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	var result map[string]interface{}
	json.Unmarshal(body, &result)

	tasks, ok := result["tasks"].([]interface{})
	if !ok || len(tasks) == 0 {
		fmt.Println("📋 当前没有任务")
		return
	}

	fmt.Printf("📋 共 %d 个任务:\n\n", len(tasks))
	for _, t := range tasks {
		taskMap, ok := t.(map[string]interface{})
		if !ok {
			continue
		}
		status := taskMap["status"].(string)
		icon := statusIcon(status)
		fmt.Printf("  %s [%s] %s", icon, taskMap["task_id"], status)
		if msg, ok := taskMap["message"].(string); ok && msg != "" {
			fmt.Printf(" - %s", msg)
		}
		fmt.Println()
	}
}

// =========================================================================
// 输出辅助
// =========================================================================

func printResponse(resp *http.Response) {
	body, _ := io.ReadAll(resp.Body)
	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		fmt.Printf("响应: %s\n", string(body))
		return
	}

	if success, ok := result["success"].(bool); ok && success {
		if val, ok := result["value"].(string); ok {
			fmt.Printf("✅ 成功: %s\n", val)
		} else {
			fmt.Println("✅ 成功")
		}
	} else {
		errMsg := "未知错误"
		if e, ok := result["error"].(string); ok {
			errMsg = e
		}
		fmt.Printf("❌ 失败: %s\n", errMsg)
		if hint, ok := result["leader_hint"].(string); ok && hint != "" {
			fmt.Printf("   提示: Leader 节点为 %s\n", hint)
		}
	}
}

func printTaskResponse(resp *http.Response) {
	body, _ := io.ReadAll(resp.Body)
	var result map[string]interface{}
	if err := json.Unmarshal(body, &result); err != nil {
		fmt.Printf("响应: %s\n", string(body))
		return
	}

	success, _ := result["success"].(bool)
	if !success {
		errMsg := "未知错误"
		if e, ok := result["error"].(string); ok {
			errMsg = e
		}
		fmt.Printf("❌ 失败: %s\n", errMsg)
		return
	}

	task, ok := result["task"].(map[string]interface{})
	if !ok {
		fmt.Println("✅ 成功")
		return
	}

	status := task["status"].(string)
	icon := statusIcon(status)

	fmt.Printf("%s 任务 [%s] 状态: %s\n", icon, task["task_id"], status)
	if msg, ok := task["message"].(string); ok && msg != "" {
		fmt.Printf("   消息: %s\n", msg)
	}
	if created, ok := task["created_at"].(string); ok && created != "" {
		fmt.Printf("   创建: %s\n", created)
	}
	if updated, ok := task["updated_at"].(string); ok && updated != "" {
		fmt.Printf("   更新: %s\n", updated)
	}
	if completed, ok := task["completed_at"].(string); ok && completed != "" {
		fmt.Printf("   完成: %s\n", completed)
	}
}

func statusIcon(status string) string {
	switch status {
	case "PENDING":
		return "⏳"
	case "RUNNING":
		return "🔄"
	case "SUCCESS":
		return "✅"
	case "FAILED":
		return "❌"
	case "CANCELLED":
		return "🚫"
	case "TIMEOUT":
		return "⏰"
	default:
		return "❓"
	}
}
