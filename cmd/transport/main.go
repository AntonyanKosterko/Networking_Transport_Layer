package main

import (
	"github.com/stayfatal/Networking_Transport_Layer/internal/adapter/httpapi"
	"github.com/stayfatal/Networking_Transport_Layer/internal/adapter/httpclient"
	kafkaclient "github.com/stayfatal/Networking_Transport_Layer/internal/adapter/kafka"
	"github.com/stayfatal/Networking_Transport_Layer/internal/config"
	"github.com/stayfatal/Networking_Transport_Layer/internal/usecase"
	"log"
	"net/http"
	"strconv"
	"time"
)

func main() {
	broker := config.Env("KAFKA_BROKER", "localhost:9092")
	topic := config.Env("KAFKA_TOPIC", "segments")
	agent := config.Env("AGENT_URL", "http://localhost:8083")
	app := config.Env("APPLICATION_URL", "http://localhost:8081")
	loss, _ := strconv.ParseFloat(config.Env("LOSS_RATE", "0.04"), 64)
	if loss < 0 || loss > 1 {
		log.Fatal("LOSS_RATE must be between 0 and 1")
	}
	s := usecase.New()
	k := kafkaclient.New(broker, topic, config.Env("KAFKA_GROUP", "prodname-transport"))
	defer k.Close()
	k.Start(s)
	poster := httpclient.New()
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for now := range ticker.C {
			for _, c := range s.Collect(now) {
				if err := poster.Post(app+"/api/v1/receive", c); err != nil {
					log.Print("delivery: ", err)
				}
			}
		}
	}()
	log.Fatal(http.ListenAndServe(":"+config.Env("PORT", "8082"), httpapi.New(s, k, poster, agent, app, loss)))
}
