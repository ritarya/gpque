package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"gpqueue/internal/mqclient"
	"gpqueue/internal/source"
)

func main() {
	cfg := loadConfig()

	src, err := source.NewCSV(cfg.csvPath, cfg.streamerID)
	if err != nil {
		log.Fatalf("open csv source: %v", err)
	}
	defer src.Close()

	mq := mqclient.New(cfg.queueAddr, cfg.streamerID)
	defer mq.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	interval := time.Duration(cfg.publishIntervalMS) * time.Millisecond
	log.Printf("streamer %s started: csv=%s interval=%v topic=%s queue=%s",
		cfg.streamerID, cfg.csvPath, interval, cfg.topicName, cfg.queueAddr)

loop:
	for {
		rec, err := src.Next(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break loop
			}
			log.Printf("source error: %v", err)
			continue
		}

		payload, err := json.Marshal(rec)
		if err != nil {
			log.Printf("marshal error: %v", err)
			continue
		}

		if err := mq.Publish(ctx, cfg.topicName, payload); err != nil {
			if ctx.Err() != nil {
				break loop
			}
			log.Printf("publish error: %v", err)
		}

		select {
		case <-ctx.Done():
			break loop
		case <-time.After(interval):
		}
	}

	log.Printf("streamer %s shut down", cfg.streamerID)
}

type config struct {
	streamerID        string
	csvPath           string
	publishIntervalMS int
	topicName         string
	queueAddr         string
}

func loadConfig() config {
	return config{
		streamerID:        getenv("STREAMER_ID", "streamer-0"),
		csvPath:           getenv("CSV_PATH", "/data/dcgm_metrics.csv"),
		publishIntervalMS: getenvInt("PUBLISH_INTERVAL_MS", 100),
		topicName:         getenv("TOPIC_NAME", "telemetry"),
		queueAddr:         getenv("QUEUE_ADDR", "http://localhost:8080"),
	}
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getenvInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		log.Printf("invalid %s=%q, using default %d", key, v, def)
		return def
	}
	return n
}
