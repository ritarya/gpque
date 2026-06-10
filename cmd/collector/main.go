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

	"gpqueue/internal/model"
	"gpqueue/internal/mqclient"
	"gpqueue/internal/storage"
)

func main() {
	cfg := loadConfig()

	repo, err := storage.New(cfg.dbDSN)
	if err != nil {
		log.Fatalf("open storage: %v", err)
	}
	defer repo.Close()

	mq := mqclient.New(cfg.queueAddr, cfg.collectorID)
	defer mq.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	waitForDB(ctx, repo, cfg.collectorID)

	log.Printf("collector %s started: topic=%s group=%s queue=%s",
		cfg.collectorID, cfg.topicName, cfg.consumerGroup, cfg.queueAddr)

	for ctx.Err() == nil {
		msgs, err := mq.Fetch(ctx, cfg.topicName, cfg.consumerGroup, cfg.batchSize)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			log.Printf("fetch error: %v", err)
			continue
		}

		for _, msg := range msgs {
			var rec model.TelemetryRecord
			if err := json.Unmarshal(msg.Payload, &rec); err != nil {
				log.Printf("malformed message at offset %d: %v — nacking", msg.Offset, err)
				if nackErr := mq.Nack(ctx, cfg.topicName, cfg.consumerGroup, msg.Offset); nackErr != nil && ctx.Err() == nil {
					log.Printf("nack error at offset %d: %v", msg.Offset, nackErr)
				}
				continue
			}

			if err := persist(ctx, repo, &rec); err != nil {
				log.Printf("persist error at offset %d (id=%s): %v — nacking", msg.Offset, rec.ID, err)
				if nackErr := mq.Nack(ctx, cfg.topicName, cfg.consumerGroup, msg.Offset); nackErr != nil && ctx.Err() == nil {
					log.Printf("nack error at offset %d: %v", msg.Offset, nackErr)
				}
				continue
			}

			if err := mq.Commit(ctx, cfg.topicName, cfg.consumerGroup, msg.Offset); err != nil && ctx.Err() == nil {
				log.Printf("commit error at offset %d: %v", msg.Offset, err)
			}
		}
	}

	log.Printf("collector %s shut down", cfg.collectorID)
}

func persist(ctx context.Context, repo storage.Repository, rec *model.TelemetryRecord) error {
	if err := repo.UpsertGPU(ctx, rec); err != nil {
		return err
	}
	return repo.InsertTelemetry(ctx, rec)
}

func waitForDB(ctx context.Context, repo storage.Repository, id string) {
	for {
		if err := repo.Ping(ctx); err == nil {
			return
		}
		log.Printf("collector %s: waiting for database...", id)
		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
}

type config struct {
	collectorID   string
	consumerGroup string
	topicName     string
	queueAddr     string
	dbDSN         string
	batchSize     int
	dlqTopic      string
	maxRetries    int
}

func loadConfig() config {
	return config{
		collectorID:   getenv("COLLECTOR_ID", "collector-0"),
		consumerGroup: getenv("CONSUMER_GROUP", "telemetry-collectors"),
		topicName:     getenv("TOPIC_NAME", "telemetry"),
		queueAddr:     getenv("QUEUE_ADDR", "http://localhost:8080"),
		dbDSN:         getenv("DB_DSN", "postgres://postgres:postgres@localhost:5432/telemetry?sslmode=disable"),
		batchSize:     getenvInt("BATCH_SIZE", 50),
		dlqTopic:      getenv("DLQ_TOPIC", "telemetry-dlq"),
		maxRetries:    getenvInt("MAX_RETRIES", 3),
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
