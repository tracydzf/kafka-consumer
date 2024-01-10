package main

import (
	"fmt"
	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
	"kafka-consumer/common/kafka"
	"kafka-consumer/scheduler"
	"log"
	"net/http"
)

func main() {
	// 创建 Kafka 生产者

	topic := "test-topic"
	cfg := &kafka.ProducerCfg{
		BrokerList: []string{"localhost:39092"},
		Topic:      topic,
		TimeoutMs:  1000,
	}
	producer, err := kafka.NewProducer(cfg)
	if err != nil {
		log.Fatal("Failed to create Kafka producer:", err)
	}

	// 创建 Gin 路由
	router := gin.Default()

	go InitConsumer(topic, "test")

	// 定义处理程序
	sendMessageHandler := func(c *gin.Context) {
		message := c.PostForm("message")
		// 发送消息到 Kafka
		err := producer.SendMessage(cfg.Topic, []byte(message))
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "Failed to send message",
			})
			return
		}
		c.JSON(http.StatusOK, gin.H{
			"message": "Message sent successfully",
		})
	}

	// 设置路由处理程序
	router.POST("/message", sendMessageHandler)

	// 启动 HTTP 服务器
	err = router.Run(":8080")
	if err != nil {
		log.Fatal("Failed to start HTTP server:", err)
	}
}

func InitConsumer(topic, taskType string) {
	consumerConfig := scheduler.KafkaConsumerCfg{
		TaskType:     taskType,
		Topic:        topic,
		MaxBatchSize: 100,
		MaxWaitTimeS: 5,
	}
	kafkaConsumer := scheduler.NewKafkaConsumer([]string{"localhost:39092"})
	_, err := kafkaConsumer.StartKafkaConsumer(consumerConfig, func(msgs []*sarama.ConsumerMessage, consumerPause scheduler.ConsumerPause) bool {
		for _, msg := range msgs {
			//var msgString string
			//err := json.Unmarshal(msg.Value, &msgString)
			//if err != nil {
			//	fmt.Print(err)
			//	fmt.Print(err)
			//	return false
			//}
			//fmt.Println(msgString)
			fmt.Print(string(msg.Value))
		}
		return true
	})

	fmt.Print(err)
	//defer consumer.Stop()
}
