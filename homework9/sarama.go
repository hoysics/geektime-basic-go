package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	consumerGroup = "my-group"
	topic         = "my-topic"

	lag = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kafka_consumer_lag",
		Help: "当前消费组进度",
	}, []string{"consumer_group"})

	offsetSuccessRate = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kafka_consumer_offset_success_rate",
		Help: "提交成功率",
	}, []string{"consumer_group"})

	consumer, _ = sarama.NewConsumerGroup([]string{"localhost:9092"}, consumerGroup, nil)
)

func init() {
	prometheus.MustRegister(lag)
	prometheus.MustRegister(offsetSuccessRate)
}

func main() {
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Fatal(http.ListenAndServe(":2112", nil))
	}()

	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		for {
			err := consumer.Consume(context.Background(), []string{topic}, &consumerHandler{})
			if err != nil {
				log.Println("Error from consumer:", err)
			}
		}
	}()

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	<-sigterm

	consumer.Close()
	wg.Wait()
}

type consumerHandler struct{}

func (h *consumerHandler) Setup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (h *consumerHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (h *consumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		// TODO 处理消息
		time.Sleep(100 * time.Millisecond)

		// 更新水位线指标
		highWatermark := claim.HighWaterMarkOffset()
		lag.WithLabelValues(consumerGroup).Set(float64(highWatermark - message.Offset))
		offsetSuccessRate.WithLabelValues(consumerGroup).Set(1.0)

		session.MarkMessage(message, "")
	}

	return nil
}
