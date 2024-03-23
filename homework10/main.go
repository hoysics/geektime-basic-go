package main

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

var (
	redisClient *redis.Client
	nodeCount   = 5
	nodes       = make([]string, nodeCount)
	loadMutex   sync.Mutex
)

func init() {
	redisClient = redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})

	for i := 0; i < nodeCount; i++ {
		nodes[i] = "node_" + strconv.Itoa(i)
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())

	// 模拟节点负载的定时变化
	go func() {
		for {
			time.Sleep(5 * time.Second)
			updateNodeLoads()
		}
	}()

	// 定时选择负载最低的节点进行热榜计算
	go func() {
		for {
			time.Sleep(10 * time.Second)
			selectedNode := selectLowestLoadNode()
			if selectedNode != "" {
				calculateHotRanking(selectedNode)
			}
		}
	}()

	select {}
}

func updateNodeLoads() {
	loadMutex.Lock()
	defer loadMutex.Unlock()

	for _, node := range nodes {
		load := rand.Intn(101) // 随机生成节点负载
		redisClient.Set(context.Background(), node, load, 0)
	}
}

func selectLowestLoadNode() string {
	loadMutex.Lock()
	defer loadMutex.Unlock()

	lowestLoad := 101
	selectedNode := ""

	for _, node := range nodes {
		load, err := redisClient.Get(context.Background(), node).Int()
		if err != nil {
			continue
		}

		if load < lowestLoad {
			lowestLoad = load
			selectedNode = node
		}
	}

	return selectedNode
}

func calculateHotRanking(node string) {
	fmt.Printf("计算热榜排名: %s\n", node)
}
