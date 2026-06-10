package storage

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"gpqueue/internal/model"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// TelemetryRow is the per-record shape returned by QueryTelemetry.
// model_name is joined from the gpus table since it is not stored in telemetry rows.
type TelemetryRow struct {
	ID          string
	ProcessedAt time.Time
	MetricName  string
	GpuID       int
	Device      string
	UUID        string
	ModelName   string
	Hostname    string
	Value       float64
	Container   *string
	Pod         *string
	Namespace   *string
}

// CursorPos is the decoded form of an opaque pagination cursor.
type CursorPos struct {
	ProcessedAt time.Time
	ID          string
}

// TelemetryQuery holds all filter + pagination parameters for QueryTelemetry.
type TelemetryQuery struct {
	GPUUUID   string
	StartTime *time.Time
	EndTime   *time.Time
	Metric    string
	Limit     int
	Cursor    *CursorPos
}

// TelemetryPage is the result of a QueryTelemetry call.
type TelemetryPage struct {
	Records    []TelemetryRow
	NextCursor string // base64 JSON; empty when this is the last page
	Total      int64
}

// Repository persists and reads telemetry records.
type Repository interface {
	// Write path (used by collector)
	UpsertGPU(ctx context.Context, rec *model.TelemetryRecord) error
	InsertTelemetry(ctx context.Context, rec *model.TelemetryRecord) error

	// Read path (used by API gateway)
	ListGPUs(ctx context.Context) ([]model.GPU, error)
	GetGPU(ctx context.Context, uuid string) (*model.GPU, error)
	QueryTelemetry(ctx context.Context, q TelemetryQuery) (TelemetryPage, error)

	Ping(ctx context.Context) error
	Close() error
}

type pgRepo struct {
	db *sql.DB
}

// New opens a connection pool to the PostgreSQL DSN.
func New(dsn string) (Repository, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)
	return &pgRepo{db: db}, nil
}

func (r *pgRepo) Ping(ctx context.Context) error {
	return r.db.PingContext(ctx)
}

func (r *pgRepo) Close() error {
	return r.db.Close()
}

// UpsertGPU inserts or updates the GPU record, advancing last_seen if newer.
func (r *pgRepo) UpsertGPU(ctx context.Context, rec *model.TelemetryRecord) error {
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO gpus (uuid, gpu_id, device, model_name, hostname, first_seen, last_seen)
		VALUES ($1, $2, $3, $4, $5, $6, $6)
		ON CONFLICT (uuid) DO UPDATE
			SET last_seen = GREATEST(gpus.last_seen, EXCLUDED.last_seen)
	`, rec.UUID, rec.GpuID, rec.Device, rec.ModelName, rec.Hostname, rec.ProcessedAt)
	return err
}

// InsertTelemetry inserts a telemetry row; idempotent via ON CONFLICT DO NOTHING.
func (r *pgRepo) InsertTelemetry(ctx context.Context, rec *model.TelemetryRecord) error {
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO telemetry (
			id, processed_at, source_timestamp, metric_name, gpu_uuid,
			gpu_id, device, hostname, value, container, pod, namespace,
			labels_raw, streamer_id
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
		ON CONFLICT (id) DO NOTHING
	`,
		rec.ID, rec.ProcessedAt, rec.SourceTimestamp, rec.MetricName, rec.UUID,
		rec.GpuID, rec.Device, rec.Hostname, rec.Value,
		rec.Container, rec.Pod, rec.Namespace, rec.LabelsRaw, rec.StreamerID,
	)
	return err
}
