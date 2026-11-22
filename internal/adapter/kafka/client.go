package kafkaclient

import (
	"context"
	"encoding/json"
	"github.com/segmentio/kafka-go"
	"github.com/stayfatal/Networking_Transport_Layer/internal/domain"
	"github.com/stayfatal/Networking_Transport_Layer/internal/usecase"
	"log"
	"sync"
	"time"
)

type Client struct {
	writer *kafka.Writer
	reader *kafka.Reader
}

func New(broker, topic, group string) *Client {
	for {
		conn, err := kafka.Dial("tcp", broker)
		if err == nil {
			err = conn.CreateTopics(kafka.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1})
			if err == nil || err == kafka.TopicAlreadyExists {
				_, err = conn.ReadPartitions(topic)
			}
			conn.Close()
		}
		if err == nil {
			break
		}
		log.Printf("waiting for Kafka topic: %v", err)
		time.Sleep(time.Second)
	}
	return &Client{writer: &kafka.Writer{Addr: kafka.TCP(broker), Topic: topic, Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireAll, AllowAutoTopicCreation: true}, reader: kafka.NewReader(kafka.ReaderConfig{Brokers: []string{broker}, Topic: topic, GroupID: group, MinBytes: 1, MaxBytes: 1e6, StartOffset: kafka.FirstOffset, WatchPartitionChanges: true, PartitionWatchInterval: time.Second})}
}
func (c *Client) Start(s *usecase.Service) {
	ready := make(chan struct{})
	var once sync.Once
	go func() {
		for {
			msg, err := c.reader.ReadMessage(context.Background())
			if err != nil {
				log.Print("kafka read: ", err)
				time.Sleep(time.Second)
				continue
			}
			if string(msg.Key) == "__ready" {
				once.Do(func() { close(ready) })
				continue
			}
			var seg domain.Segment
			if json.Unmarshal(msg.Value, &seg) == nil {
				s.Accept(seg)
			}
		}
	}()
	go func() {
		for {
			select {
			case <-ready:
				return
			default:
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			if err := c.writer.WriteMessages(ctx, kafka.Message{Key: []byte("__ready"), Value: []byte("ready")}); err != nil {
				log.Printf("waiting for Kafka consumer: %v", err)
			}
			cancel()
			time.Sleep(time.Second)
		}
	}()
	<-ready
}
func (c *Client) Publish(ctx context.Context, seg domain.Segment) error {
	b, err := json.Marshal(seg)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return c.writer.WriteMessages(ctx, kafka.Message{Key: []byte(usecase.Key(usecase.ManifestFor(seg))), Value: b})
}
func (c *Client) Close() { c.reader.Close(); c.writer.Close() }
