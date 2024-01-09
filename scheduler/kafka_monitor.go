package scheduler

import (
	"fmt"
	"github.com/IBM/sarama"
	"kafka-consumer/common/kafka"
	"kafka-consumer/proto"
	"kafka-consumer/util/closer"
)

// KafkaTopicMonitor kafka monitor
type KafkaTopicMonitor struct {
	closer.Closer
	taskType string
	topic    string
	monitor  *kafka.Monitor
}

// NewKafkaTopicMonitor returns kafka topic monitor
func NewKafkaTopicMonitor(taskType string, clusterID proto.ClusterID, cfg *KafkaConfig) (*KafkaTopicMonitor, error) {
	consumer, err := sarama.NewConsumer(cfg.BrokerList, defaultKafkaCfg())
	if err != nil {
		return nil, err
	}

	partitions, err := consumer.Partitions(cfg.Topic)
	if err != nil {
		return nil, fmt.Errorf("get partitions: err[%w]", err)
	}

	// create kafka monitor
	monitor, err := kafka.NewKafkaMonitor(clusterID, "SCHEDULER", cfg.BrokerList, cfg.Topic, partitions, kafka.DefaultIntervalSecs)
	if err != nil {
		return nil, fmt.Errorf("new kafka monitor: broker list[%v], topic[%v], parts[%v], error[%w]", cfg.BrokerList, cfg.Topic, partitions, err)
	}

	return &KafkaTopicMonitor{
		Closer:   closer.New(),
		taskType: taskType,
		topic:    cfg.Topic,
		monitor:  monitor,
	}, nil
}

func (m *KafkaTopicMonitor) Close() {
	m.Closer.Close()
	m.monitor.Close()
}

func defaultKafkaCfg() *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.Version = kafka.DefaultKafkaVersion
	cfg.Consumer.Return.Errors = true                   //设置消费者在发生错误时是否将其返回。
	cfg.Producer.Return.Successes = true                //设置生产者是否在成功发送消息后将其返回
	cfg.Producer.RequiredAcks = sarama.WaitForAll       //表示等待所有副本都确认消息。
	cfg.Producer.Compression = sarama.CompressionSnappy //表示使用 Snappy 压缩算法
	return cfg
}
