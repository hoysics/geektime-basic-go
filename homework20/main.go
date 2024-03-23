package main

import (
	"fmt"
)

/**
作为产品经理，可以下几个角度来判断一个用户是否是活跃用户：
活跃时间段： 用户在一定时间段内的活跃次数，比如每周、每月的活跃天数或操作次数。
互动行为： 用户与平台互动的频率，包括登录次数、发布内容、点赞、评论等操作。
内容消费： 用户对于平台内容的浏览、阅读、收藏等行为。
*/

type User struct {
	ID       int
	Username string
	IsActive bool
}

type Inbox struct {
	Messages []string
}

type FeedService struct {
	Users   map[int]*User
	Inboxes map[int]*Inbox
}

func NewFeedService() *FeedService {
	return &FeedService{
		Users:   make(map[int]*User),
		Inboxes: make(map[int]*Inbox),
	}
}

func (fs *FeedService) PostMessage(userID int, message string) {
	if user, ok := fs.Users[userID]; ok {
		if user.IsActive {
			// 对活跃用户进行写扩散
			for _, followerID := range fs.getFollowers(userID) {
				fs.addToInbox(followerID, message)
			}
		} else {
			// 非活跃用户走原逻辑
			fs.addToInbox(userID, message)
		}
	} else {
		fmt.Println("User not found")
	}
}

func (fs *FeedService) addToInbox(userID int, message string) {
	if inbox, ok := fs.Inboxes[userID]; ok {
		inbox.Messages = append(inbox.Messages, message)
	} else {
		fs.Inboxes[userID] = &Inbox{Messages: []string{message}}
	}
}

func (fs *FeedService) getFollowers(userID int) []int {
	// 模拟获取用户的粉丝列表
	return []int{1, 2, 3} // 假设用户1, 2, 3是userID的粉丝
}

func main() {
	feedService := NewFeedService()

	// 模拟创建几个用户
	user1 := &User{ID: 1, Username: "user1", IsActive: true}
	user2 := &User{ID: 2, Username: "user2", IsActive: false}

	feedService.Users[user1.ID] = user1
	feedService.Users[user2.ID] = user2

	// 发布消息
	feedService.PostMessage(user1.ID, "Hello, world!")
	feedService.PostMessage(user2.ID, "Hi there!")

	// 打印收件箱消息
	for userID, inbox := range feedService.Inboxes {
		fmt.Printf("User %d's Inbox:\n", userID)
		for _, message := range inbox.Messages {
			fmt.Println(message)
		}
	}
}
