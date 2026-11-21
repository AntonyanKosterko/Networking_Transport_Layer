package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
	"math/rand"
	"net/http"
	"prodname/common"
	"strconv"
	"sync"
	"time"
)

type assembly struct {
	manifest common.Manifest
	parts    map[int][]byte
	last     time.Time
	emitted  bool
}
type state struct {
	sync.Mutex
	items  map[string]*assembly
	failed map[string]time.Time
}

var st = state{items: map[string]*assembly{}, failed: map[string]time.Time{}}

func key(m common.Manifest) string {
	return fmt.Sprintf("%s|%s|%s|%d", m.RequestID, m.SentAt, m.Kind, m.ChunkIndex)
}
func main() {
	broker := common.Env("KAFKA_BROKER", "localhost:9092")
	topic := common.Env("KAFKA_TOPIC", "segments")
	agent := common.Env("AGENT_URL", "http://localhost:8083")
	app := common.Env("APPLICATION_URL", "http://localhost:8081")
	loss, _ := strconv.ParseFloat(common.Env("LOSS_RATE", "0.04"), 64)
	if loss < 0 || loss > 1 {
		log.Fatal("LOSS_RATE must be between 0 and 1")
	}
	// Create the topic before the consumer joins its group, so the first
	// assignment includes its partition even on a fresh broker.
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
	writer := &kafka.Writer{Addr: kafka.TCP(broker), Topic: topic, Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireAll, AllowAutoTopicCreation: true}
	defer writer.Close()
	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: []string{broker}, Topic: topic, GroupID: common.Env("KAFKA_GROUP", "prodname-transport"), MinBytes: 1, MaxBytes: 1e6, StartOffset: kafka.FirstOffset, WatchPartitionChanges: true, PartitionWatchInterval: time.Second})
	defer reader.Close()
	ready := make(chan struct{})
	var readyOnce sync.Once
	go func() {
		for {
			msg, err := reader.ReadMessage(context.Background())
			if err != nil {
				log.Print("kafka read: ", err)
				time.Sleep(time.Second)
				continue
			}
			if string(msg.Key) == "__ready" {
				readyOnce.Do(func() { close(ready) })
				continue
			}
			var s common.Segment
			if json.Unmarshal(msg.Value, &s) != nil {
				continue
			}
			m := common.Manifest{Request: common.Request{RequestID: s.RequestID, SentAt: s.SentAt}, Kind: s.Kind, ChunkIndex: s.ChunkIndex, ChunkTotal: s.ChunkTotal, SegmentTotal: s.SegmentTotal}
			st.Lock()
			a := st.items[key(m)]
			if a != nil && !a.emitted && st.failed[s.RequestID].IsZero() && a.manifest.SegmentTotal == s.SegmentTotal {
				if _, ok := a.parts[s.SegmentNumber]; !ok {
					a.parts[s.SegmentNumber] = s.Payload
					a.last = time.Now()
				}
			}
			st.Unlock()
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
			if err := writer.WriteMessages(ctx, kafka.Message{Key: []byte("__ready"), Value: []byte("ready")}); err != nil {
				log.Printf("waiting for Kafka consumer: %v", err)
			}
			cancel()
			time.Sleep(time.Second)
		}
	}()
	<-ready
	go func() {
		t := time.NewTicker(2 * time.Second)
		defer t.Stop()
		for range t.C {
			var outgoing []common.Chunk
			st.Lock()
			for id, a := range st.items {
				if time.Since(a.last) > 10*time.Minute {
					delete(st.items, id)
					continue
				}
				m := a.manifest
				if a.emitted || !st.failed[m.RequestID].IsZero() {
					continue
				}
				if len(a.parts) == m.SegmentTotal {
					if m.Kind == "names" {
						description := st.items[fmt.Sprintf("%s|%s|description|0", m.RequestID, m.SentAt)]
						if description == nil || !description.emitted {
							continue
						}
					}
					data := []byte{}
					for n := 1; n <= m.SegmentTotal; n++ {
						data = append(data, a.parts[n]...)
					}
					a.emitted = true
					if m.Kind == "names" {
						var names []string
						if json.Unmarshal(data, &names) != nil || len(names) < 2 || len(names) > 3 {
							st.failed[m.RequestID] = time.Now()
							outgoing = append(outgoing, common.Chunk{Request: m.Request, IsError: true, Payload: []string{}, ErrorMessage: "Некорректная порция"})
						} else {
							outgoing = append(outgoing, common.Chunk{Request: m.Request, ChunkIndex: m.ChunkIndex, ChunkTotal: m.ChunkTotal, Payload: names})
						}
					}
				} else if time.Since(a.last) >= 6*time.Second {
					a.emitted = true
					st.failed[m.RequestID] = time.Now()
					outgoing = append(outgoing, common.Chunk{Request: m.Request, ChunkIndex: m.ChunkIndex, ChunkTotal: m.ChunkTotal, IsError: true, Payload: []string{}, ErrorMessage: "Потеряны сегменты сообщения"})
				}
			}
			for id, at := range st.failed {
				if time.Since(at) > 10*time.Minute {
					delete(st.failed, id)
				}
			}
			st.Unlock()
			for _, c := range outgoing {
				if e := common.Post(app+"/api/v1/receive", c); e != nil {
					log.Print("delivery: ", e)
				}
			}
		}
	}()
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/input", common.Only("POST", func(w http.ResponseWriter, r *http.Request) {
		var req common.Request
		if common.Decode(r, &req) != nil || req.Validate() != nil {
			common.Error(w, 400, "invalid request")
			return
		}
		if err := common.Post(agent+"/api/v1/process", req); err != nil {
			common.Error(w, 503, err.Error())
			return
		}
		common.JSON(w, 202, map[string]string{"request_id": req.RequestID, "status": "accepted"})
	}))
	mux.HandleFunc("/api/v1/manifest", common.Only("POST", func(w http.ResponseWriter, r *http.Request) {
		var m common.Manifest
		if common.Decode(r, &m) != nil || m.Request.Validate() != nil || m.SegmentTotal < 1 || m.SegmentTotal > 100 || !(m.Kind == "description" || m.Kind == "names") || (m.Kind == "names" && (m.ChunkIndex < 1 || m.ChunkIndex > m.ChunkTotal || m.ChunkTotal > 5)) {
			common.Error(w, 400, "invalid manifest")
			return
		}
		st.Lock()
		k := key(m)
		if _, ok := st.items[k]; ok {
			st.Unlock()
			common.Error(w, 409, "manifest exists")
			return
		}
		st.items[k] = &assembly{manifest: m, parts: map[int][]byte{}, last: time.Now()}
		st.Unlock()
		common.JSON(w, 202, map[string]string{"status": "accepted"})
	}))
	mux.HandleFunc("/api/v1/send", common.Only("POST", func(w http.ResponseWriter, r *http.Request) {
		var s common.Segment
		if common.Decode(r, &s) != nil || (s.RequestID == "" || s.SentAt == "") || len(s.Payload) > 1000 || len(s.Payload) == 0 || s.SegmentNumber < 1 || s.SegmentNumber > s.SegmentTotal {
			common.Error(w, 400, "invalid segment")
			return
		}
		m := common.Manifest{Request: common.Request{RequestID: s.RequestID, SentAt: s.SentAt}, Kind: s.Kind, ChunkIndex: s.ChunkIndex, ChunkTotal: s.ChunkTotal, SegmentTotal: s.SegmentTotal}
		st.Lock()
		a, ok := st.items[key(m)]
		valid := ok && a.manifest.SegmentTotal == s.SegmentTotal && a.manifest.ChunkTotal == s.ChunkTotal
		st.Unlock()
		if !valid {
			common.Error(w, 400, "manifest missing or inconsistent")
			return
		}
		status := "dropped"
		if rand.Float64() >= loss {
			b, _ := json.Marshal(s)
			ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
			err := writer.WriteMessages(ctx, kafka.Message{Key: []byte(key(m)), Value: b})
			cancel()
			if err != nil {
				common.Error(w, 503, "kafka unavailable")
				return
			}
			status = "queued"
		}
		common.JSON(w, 202, map[string]any{"request_id": s.RequestID, "segment_number": s.SegmentNumber, "status": status})
	}))
	mux.HandleFunc("/api/v1/failure", common.Only("POST", func(w http.ResponseWriter, r *http.Request) {
		var c common.Chunk
		if common.Decode(r, &c) != nil || c.Request.Validate() != nil {
			common.Error(w, 400, "invalid error")
			return
		}
		st.Lock()
		already := !st.failed[c.RequestID].IsZero()
		st.failed[c.RequestID] = time.Now()
		st.Unlock()
		if !already {
			c.IsError = true
			c.Payload = []string{}
			if err := common.Post(app+"/api/v1/receive", c); err != nil {
				common.Error(w, 503, err.Error())
				return
			}
		}
		common.JSON(w, 202, map[string]string{"status": "accepted"})
	}))
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })
	log.Fatal(http.ListenAndServe(":"+common.Env("PORT", "8082"), mux))
}
