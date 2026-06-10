package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humachi"
	"github.com/go-chi/chi/v5"

	"gpqueue/internal/api"
	"gpqueue/internal/storage"
)

func main() {
	dbDSN := getenv("DB_DSN", "postgres://postgres:postgres@localhost:5432/telemetry?sslmode=disable")
	port := getenv("GATEWAY_PORT", "8080")

	repo, err := storage.New(dbDSN)
	if err != nil {
		log.Fatalf("open storage: %v", err)
	}
	defer repo.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	r := chi.NewRouter()

	// /healthz always returns 200 — the process is alive.
	// /readyz checks the DB on each call — fails (503) until postgres is ready.
	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	r.Get("/readyz", func(w http.ResponseWriter, req *http.Request) {
		if err := repo.Ping(req.Context()); err != nil {
			http.Error(w, "db unavailable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	// Huma handles /api/v1/..., /docs, /openapi.json.
	config := huma.DefaultConfig("GPU Telemetry API", "1.0.0")
	config.Info.Description = "REST API for querying DCGM GPU telemetry ingested through the gpqueue pipeline."
	humaAPI := humachi.New(r, config)
	api.Register(humaAPI, repo)

	srv := &http.Server{
		Addr:    ":" + port,
		Handler: r,
	}

	// Start the HTTP server before waiting for the DB so liveness probes pass
	// immediately and readiness probes gate traffic until postgres is up.
	go func() {
		log.Printf("gateway listening on :%s  —  docs: http://localhost:%s/docs", port, port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %v", err)
		}
	}()

	// Log when the DB becomes reachable (informational only).
	go func() {
		for {
			if err := repo.Ping(ctx); err == nil {
				log.Printf("gateway: database ready")
				return
			}
			log.Printf("gateway: waiting for database...")
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}
		}
	}()

	<-ctx.Done()
	log.Printf("gateway shutting down...")
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutCancel()
	if err := srv.Shutdown(shutCtx); err != nil {
		log.Printf("graceful shutdown error: %v", err)
	}
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
