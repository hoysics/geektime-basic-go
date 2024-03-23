package main

import (
	"fmt"
	"time"
)

// Payment 模拟支付结构体
type Payment struct {
	ID     int
	Amount float64
	Status string
}

// Message 表示要发送到Kafka的消息
type Message struct {
	PaymentID int
	Content   string
}

// LocalMessageTable 本地消息表，用于存储支付消息
var LocalMessageTable map[int]Payment

// KafkaChannel 模拟Kafka消息通道
var KafkaChannel chan Message

func init() {
	LocalMessageTable = make(map[int]Payment)
	KafkaChannel = make(chan Message)
}

// MockPaymentCallback 模拟支付回调，更新本地状态并发送消息到Kafka
func MockPaymentCallback(paymentID int, amount float64) {
	payment := Payment{
		ID:     paymentID,
		Amount: amount,
		Status: "Success",
	}

	LocalMessageTable[paymentID] = payment

	// 发送消息到Kafka
	message := Message{
		PaymentID: paymentID,
		Content:   fmt.Sprintf("Payment ID: %d, Amount: %.2f", paymentID, amount),
	}
	KafkaChannel <- message
}

// RetryFailedMessages 定时重试发送失败的消息
func RetryFailedMessages() {
	for {
		select {
		case message := <-KafkaChannel:
			// 模拟消息发送失败的
			fmt.Printf("Failed to send message: %s\n", message.Content)
			time.Sleep(time.Second) // 等待一段时间后重试
		}
	}
}

func main() {
	go RetryFailedMessages()

	// 模拟支付回调
	MockPaymentCallback(1, 100.0)

	time.Sleep(30 * time.Second)

	// 关闭Kafka通道
	close(KafkaChannel)
}
