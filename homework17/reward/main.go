package main

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// RewardService 模拟奖励服务
type RewardService struct {
	RedisClient *redis.Client
}

// RecordReward 记录奖励
func (rs *RewardService) RecordReward(biz, bizID string) {
	key := fmt.Sprintf("reward:%s:%s", biz, bizID)

	// 利用 Redis 的 SetNX 方法实现幂等性
	ok, err := rs.RedisClient.SetNX(context.Background(), key, true, 24*time.Hour).Result()
	if err != nil {
		fmt.Println("Error checking:", err)
		return
	}

	if ok {
		// 如返回值为 true，表示该业务+业务ID之前没有记录过奖励，执行记账操作
		fmt.Printf("刷新记录 biz %s, bizID %s\n", biz, bizID)
		// TODO: 执行记账操作

		// 后续逻
	} else {
		// 如返回值为 false，表示该业务+业务ID之前已经记录过奖励，不执行记账操作
		fmt.Printf("重复记录 biz %s, bizID %s\n", biz, bizID)
	}
}

func main() {
	redisClient := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
	})

	// 实例化奖励服务
	rewardService := &RewardService{
		RedisClient: redisClient,
	}

	// 模拟收到支付成功消息后记录奖励
	rewardService.RecordReward("exampleBiz", "12345")

	time.Sleep(1 * time.Second)

	_ = redisClient.Close()
}
