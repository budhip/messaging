package kafka

import (
	"errors"
	"io"
	"log"
	"sync"

	"github.com/Shopify/sarama"
)

var ErrInactiveConnection = errors.New("inactive connection")

type Client struct {
	client        sarama.Client
	config        *Config
	producer      sarama.SyncProducer
	subscriptions sync.Map
}

type Config struct {
	*sarama.Config
	AutoAck bool
}

type SubscriptionHandler struct {
	kc       *Client
	name     string
	consumer io.Closer
}

func NewClient(address []string, config *Config) (*Client, error) {
	if config == nil {
		saramaCfg := sarama.NewConfig()
		config = &Config{
			Config:  saramaCfg,
			AutoAck: true,
		}
	}
	//config.Version = sarama.V3_3_2_0
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Consumer.Return.Errors = true

	client, err := sarama.NewClient(address, config.Config)
	if err != nil {
		return nil, err
	}

	return &Client{client: client, config: config}, nil
}

func (ksh *SubscriptionHandler) Close() error {
	if ksh.consumer != nil {
		ksh.kc.removeHandler(ksh.name)
		return ksh.consumer.Close()
	}
	return nil
}

func (kc *Client) removeHandler(key string) {
	kc.subscriptions.Delete(key)
}

func (kc *Client) Close() error {
	if kc.client == nil {
		return nil
	}

	func() {
		kc.subscriptions.Range(func(key, value interface{}) bool {
			subsHandler, ok := value.(*SubscriptionHandler)
			if ok {
				_ = subsHandler.Close()
				return true
			}

			return false
		})
	}()

	if kc.producer != nil {
		if err := kc.producer.Close(); err != nil {
			log.Print(err)
		}
		kc.producer = nil
	}

	if err := kc.client.Close(); err != nil {
		return err
	}

	kc.client = nil
	return nil
}

func (kc *Client) Publish(topic string, message []byte) error {
	if kc.producer == nil {
		producer, err := sarama.NewSyncProducerFromClient(kc.client)
		if err != nil {
			return err
		}

		kc.producer = producer
	}

	msg := sarama.ProducerMessage{Topic: topic, Value: sarama.ByteEncoder(message)}
	if _, _, err := kc.producer.SendMessage(&msg); err != nil {
		log.Print(err)
		return err
	}

	return nil
}
