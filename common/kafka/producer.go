package kafka

import (
	"github.com/IBM/sarama"
	"log"
	"time"
)

var DefaultKafkaVersion = sarama.V2_1_0_0

type MsgProducer interface {
	SendMessage(topic string, msg []byte) (err error)
	SendMessages(topic string, msgs [][]byte) (err error)
}

type ProducerCfg struct {
	BrokerList []string `json:"broker_list"`
	Topic      string   `json:"topic"`
	TimeoutMs  int64    `json:"timeout_ms"`
}

type Producer struct {
	sarama.SyncProducer
}

func defaultCfg() *sarama.Config {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Version = DefaultKafkaVersion
	return config
}

func NewProducer(cfg *ProducerCfg) (*Producer, error) {
	config := defaultCfg()
	if cfg.TimeoutMs <= 0 {
		cfg.TimeoutMs = 1000
	}
	config.Producer.Timeout = time.Duration(cfg.TimeoutMs) * time.Millisecond

	producer, err := sarama.NewSyncProducer(cfg.BrokerList, config)
	if err != nil {
		log.Fatal("sarama.NewClient:", err)
		return nil, err
	}
	return &Producer{producer}, err
}

func (p *Producer) SendMessage(topic string, msg []byte) (err error) {
	m := &sarama.ProducerMessage{
		Topic:     topic,
		Timestamp: time.Now(),
		Value:     sarama.ByteEncoder(msg),
	}
	_, _, err = p.SyncProducer.SendMessage(m)
	return err
}

func (p *Producer) SendMessages(topic string, msgs [][]byte) (err error) {
	sendMsgs := make([]*sarama.ProducerMessage, len(msgs))
	for idx, msg := range msgs {
		sendMsgs[idx] = &sarama.ProducerMessage{
			Topic:     topic,
			Timestamp: time.Now(),
			Value:     sarama.ByteEncoder(msg),
		}
	}
	return p.SyncProducer.SendMessages(sendMsgs)
}
