package storage

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"gpqueue/internal/model"
)

func (r *pgRepo) ListGPUs(ctx context.Context) ([]model.GPU, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT uuid, gpu_id, device, model_name, hostname, first_seen, last_seen
		 FROM gpus ORDER BY uuid`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var gpus []model.GPU
	for rows.Next() {
		var g model.GPU
		if err := rows.Scan(&g.UUID, &g.GpuID, &g.Device, &g.ModelName,
			&g.Hostname, &g.FirstSeen, &g.LastSeen); err != nil {
			return nil, err
		}
		gpus = append(gpus, g)
	}
	return gpus, rows.Err()
}

func (r *pgRepo) GetGPU(ctx context.Context, uuid string) (*model.GPU, error) {
	var g model.GPU
	err := r.db.QueryRowContext(ctx,
		`SELECT uuid, gpu_id, device, model_name, hostname, first_seen, last_seen
		 FROM gpus WHERE uuid = $1`, uuid).
		Scan(&g.UUID, &g.GpuID, &g.Device, &g.ModelName,
			&g.Hostname, &g.FirstSeen, &g.LastSeen)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &g, nil
}

func (r *pgRepo) QueryTelemetry(ctx context.Context, q TelemetryQuery) (TelemetryPage, error) {
	// Build shared WHERE conditions (no cursor) for the count query.
	var baseConds []string
	var baseArgs []any

	push := func(cond string, val any) {
		baseArgs = append(baseArgs, val)
		baseConds = append(baseConds, fmt.Sprintf(cond, len(baseArgs)))
	}

	push("t.gpu_uuid = $%d", q.GPUUUID)
	if q.StartTime != nil {
		push("t.processed_at >= $%d", *q.StartTime)
	}
	if q.EndTime != nil {
		push("t.processed_at <= $%d", *q.EndTime)
	}
	if q.Metric != "" {
		push("t.metric_name = $%d", q.Metric)
	}

	baseWhere := strings.Join(baseConds, " AND ")

	var total int64
	if err := r.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM telemetry t WHERE "+baseWhere, baseArgs...).
		Scan(&total); err != nil {
		return TelemetryPage{}, fmt.Errorf("count telemetry: %w", err)
	}

	// Build data query — adds cursor condition on top of base conditions.
	dataConds := append([]string(nil), baseConds...)
	dataArgs := append([]any(nil), baseArgs...)

	if q.Cursor != nil {
		dataArgs = append(dataArgs, q.Cursor.ProcessedAt, q.Cursor.ID)
		n1, n2 := len(dataArgs)-1, len(dataArgs)
		dataConds = append(dataConds, fmt.Sprintf(
			"(t.processed_at < $%d OR (t.processed_at = $%d AND t.id::text < $%d))",
			n1, n1, n2))
	}

	limit := q.Limit
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	dataArgs = append(dataArgs, limit+1) // fetch one extra to detect next page

	dataSQL := `SELECT t.id, t.processed_at, t.metric_name, t.gpu_uuid,
		           t.gpu_id, t.device, t.hostname, t.value,
		           t.container, t.pod, t.namespace, g.model_name
		    FROM telemetry t
		    JOIN gpus g ON g.uuid = t.gpu_uuid
		    WHERE ` + strings.Join(dataConds, " AND ") +
		fmt.Sprintf(" ORDER BY t.processed_at DESC, t.id DESC LIMIT $%d", len(dataArgs))

	rows, err := r.db.QueryContext(ctx, dataSQL, dataArgs...)
	if err != nil {
		return TelemetryPage{}, fmt.Errorf("query telemetry: %w", err)
	}
	defer rows.Close()

	var records []TelemetryRow
	for rows.Next() {
		var rec TelemetryRow
		if err := rows.Scan(
			&rec.ID, &rec.ProcessedAt, &rec.MetricName, &rec.UUID,
			&rec.GpuID, &rec.Device, &rec.Hostname, &rec.Value,
			&rec.Container, &rec.Pod, &rec.Namespace, &rec.ModelName,
		); err != nil {
			return TelemetryPage{}, fmt.Errorf("scan telemetry: %w", err)
		}
		records = append(records, rec)
	}
	if err := rows.Err(); err != nil {
		return TelemetryPage{}, err
	}

	var nextCursor string
	if len(records) > limit {
		records = records[:limit]
		last := records[limit-1]
		nextCursor = EncodeCursor(last.ProcessedAt, last.ID)
	}

	return TelemetryPage{Records: records, NextCursor: nextCursor, Total: total}, nil
}

type cursorPayload struct {
	TS string `json:"ts"`
	ID string `json:"id"`
}

// EncodeCursor encodes a pagination position as a URL-safe base64 JSON string.
func EncodeCursor(processedAt time.Time, id string) string {
	b, _ := json.Marshal(cursorPayload{
		TS: processedAt.UTC().Format(time.RFC3339Nano),
		ID: id,
	})
	return base64.URLEncoding.EncodeToString(b)
}

// DecodeCursor decodes an opaque cursor string back into a CursorPos.
func DecodeCursor(s string) (*CursorPos, error) {
	b, err := base64.URLEncoding.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("decode cursor: %w", err)
	}
	var p cursorPayload
	if err := json.Unmarshal(b, &p); err != nil {
		return nil, fmt.Errorf("unmarshal cursor: %w", err)
	}
	t, err := time.Parse(time.RFC3339Nano, p.TS)
	if err != nil {
		return nil, fmt.Errorf("parse cursor timestamp: %w", err)
	}
	return &CursorPos{ProcessedAt: t, ID: p.ID}, nil
}
