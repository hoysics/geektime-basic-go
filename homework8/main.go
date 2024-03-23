package main

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"sort"
	"strconv"
)

type LikeData struct {
	ItemID  int
	LikeNum int
}

func main() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	data := []LikeData{
		{ItemID: 1, LikeNum: 20},
		{ItemID: 2, LikeNum: 15},
		{ItemID: 3, LikeNum: 25},
		{ItemID: 4, LikeNum: 10},
		{ItemID: 5, LikeNum: 30},
	}

	for _, d := range data {
		err := rdb.ZAdd(ctx, "likes", redis.Z{Score: float64(d.LikeNum), Member: strconv.Itoa(d.ItemID)}).Err()
		if err != nil {
			fmt.Println("Error adding data to Redis Sorted Set:", err)
		}
	}

	N := 3
	likeDataMap := make(map[int]int)
	likeNumsCmd := rdb.ZRevRangeWithScores(ctx, "likes", 0, int64(N-1))
	likeNums, err := likeNumsCmd.Result()
	if err != nil {
		fmt.Println("Error getting top likes from Redis Sorted Set:", err)
	}

	for _, likeNum := range likeNums {
		itemID, _ := strconv.Atoi(likeNum.Member.(string))
		likeDataMap[itemID] = int(likeNum.Score)
	}

	sortedLikeData := make([]LikeData, 0)
	for itemID, likeNum := range likeDataMap {
		sortedLikeData = append(sortedLikeData, LikeData{ItemID: itemID, LikeNum: likeNum})
	}
	sort.Slice(sortedLikeData, func(i, j int) bool {
		return sortedLikeData[i].LikeNum > sortedLikeData[j].LikeNum
	})

	fmt.Printf("点赞数量前 %d 的数据为：\n", N)
	for i := 0; i < N; i++ {
		fmt.Printf("ItemID: %d, LikeNum: %d\n", sortedLikeData[i].ItemID, sortedLikeData[i].LikeNum)
	}
}
