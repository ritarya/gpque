package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"gpqueue/internal/mq/server"
)

func main() {
	cfg := loadConfig()

	if err := os.MkdirAll(filepath.Dir(cfg.walPath), 0o755); err != nil {
		log.Fatalf("create wal dir: %v", err)
	}

	srv, err := server.New(server.Config{
		WALPath:       cfg.walPath,
		OffsetPath:    cfg.walPath + ".offsets.json",
		HighWaterMark: cfg.highWaterMark,
		MaxRetries:    cfg.maxRetries,
		PollTimeoutMS: cfg.pollTimeoutMS,
	})
	if err != nil {
		log.Fatalf("init queue server: %v", err)
	}

	httpSrv := &http.Server{
		Addr:    ":" + strconv.Itoa(cfg.port),
		Handler: srv,
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	go func() {
		log.Printf("mq listening on :%d  wal=%s hwm=%d", cfg.port, cfg.walPath, cfg.highWaterMark)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("http listen: %v", err)
		}
	}()

	<-ctx.Done()
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutCancel()
	if err := httpSrv.Shutdown(shutCtx); err != nil {
		log.Printf("http shutdown: %v", err)
	}
	srv.Close()
	log.Println("mq shut down cleanly")
}

type config struct {
	port          int
	walPath       string
	highWaterMark int
	maxRetries    int
	pollTimeoutMS int
}

func loadConfig() config {
	return config{
		port:          getenvInt("MQ_PORT", 8080),
		walPath:       getenv("MQ_WAL_PATH", "/data/wal.log"),
		highWaterMark: getenvInt("MQ_HIGH_WATER_MARK", 100000),
		maxRetries:    getenvInt("MQ_MAX_RETRIES", 3),
		pollTimeoutMS: getenvInt("MQ_POLL_TIMEOUT_MS", 5000),
	}
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getenvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
