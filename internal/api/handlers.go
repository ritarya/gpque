package api

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/danielgtaylor/huma/v2"

	"gpqueue/internal/model"
	"gpqueue/internal/storage"
)

// TelemetryRecord is the per-row shape in the telemetry response.
type TelemetryRecord struct {
	ID          string    `json:"id" doc:"Record UUID"`
	ProcessedAt time.Time `json:"processed_at" doc:"Wall-clock time the streamer ingested this record"`
	MetricName  string    `json:"metric_name"`
	GpuID       int       `json:"gpu_id"`
	Device      string    `json:"device"`
	UUID        string    `json:"uuid" doc:"GPU hardware UUID"`
	ModelName   string    `json:"model_name"`
	Hostname    string    `json:"hostname"`
	Value       float64   `json:"value"`
	Container   *string   `json:"container,omitempty"`
	Pod         *string   `json:"pod,omitempty"`
	Namespace   *string   `json:"namespace,omitempty"`
}

// ── List GPUs ────────────────────────────────────────────────────────────────

type ListGPUsInput struct{}

type ListGPUsOutput struct {
	Body struct {
		GPUs  []model.GPU `json:"gpus"`
		Total int         `json:"total"`
	}
}

// ── Get GPU telemetry ────────────────────────────────────────────────────────

type GetTelemetryInput struct {
	ID        string `path:"id"          doc:"GPU hardware UUID"`
	StartTime string `query:"start_time" doc:"ISO 8601 lower bound on processed_at (inclusive)"`
	EndTime   string `query:"end_time"   doc:"ISO 8601 upper bound on processed_at (inclusive)"`
	Metric    string `query:"metric"     doc:"Filter by DCGM metric name"`
	Limit     int    `query:"limit"      doc:"Page size (default 100, max 1000)" minimum:"1" maximum:"1000"`
	Cursor    string `query:"cursor"     doc:"Opaque pagination cursor from previous response"`
}

type GetTelemetryOutput struct {
	Body struct {
		GPUUUID    string            `json:"gpu_uuid"`
		Records    []TelemetryRecord `json:"records"`
		NextCursor string            `json:"next_cursor,omitempty"`
		Total      int64             `json:"total"`
	}
}

// Register wires all API routes onto the huma.API instance.
func Register(api huma.API, repo storage.Repository) {
	huma.Register(api, huma.Operation{
		Method:      http.MethodGet,
		Path:        "/api/v1/gpus",
		Summary:     "List all GPUs",
		Description: "Returns every distinct GPU that has sent at least one telemetry record.",
		OperationID: "list-gpus",
		Tags:        []string{"GPUs"},
	}, func(ctx context.Context, _ *ListGPUsInput) (*ListGPUsOutput, error) {
		gpus, err := repo.ListGPUs(ctx)
		if err != nil {
			return nil, huma.Error500InternalServerError("failed to list GPUs", err)
		}
		if gpus == nil {
			gpus = []model.GPU{}
		}
		out := &ListGPUsOutput{}
		out.Body.GPUs = gpus
		out.Body.Total = len(gpus)
		return out, nil
	})

	huma.Register(api, huma.Operation{
		Method:      http.MethodGet,
		Path:        "/api/v1/gpus/{id}/telemetry",
		Summary:     "Get telemetry for a GPU",
		Description: "Returns paginated telemetry records for the GPU identified by its hardware UUID.",
		OperationID: "get-gpu-telemetry",
		Tags:        []string{"Telemetry"},
		Errors:      []int{400, 404},
	}, func(ctx context.Context, input *GetTelemetryInput) (*GetTelemetryOutput, error) {
		// Confirm GPU exists.
		gpu, err := repo.GetGPU(ctx, input.ID)
		if err != nil {
			return nil, huma.Error500InternalServerError("failed to look up GPU", err)
		}
		if gpu == nil {
			return nil, huma.Error404NotFound(fmt.Sprintf("GPU %q not found", input.ID))
		}

		q := storage.TelemetryQuery{
			GPUUUID: input.ID,
			Metric:  input.Metric,
			Limit:   input.Limit,
		}

		if input.StartTime != "" {
			t, err := time.Parse(time.RFC3339, input.StartTime)
			if err != nil {
				return nil, huma.Error400BadRequest("invalid start_time — use ISO 8601 / RFC 3339", err)
			}
			q.StartTime = &t
		}
		if input.EndTime != "" {
			t, err := time.Parse(time.RFC3339, input.EndTime)
			if err != nil {
				return nil, huma.Error400BadRequest("invalid end_time — use ISO 8601 / RFC 3339", err)
			}
			q.EndTime = &t
		}
		if input.Cursor != "" {
			pos, err := storage.DecodeCursor(input.Cursor)
			if err != nil {
				return nil, huma.Error400BadRequest("invalid cursor", err)
			}
			q.Cursor = pos
		}

		page, err := repo.QueryTelemetry(ctx, q)
		if err != nil {
			return nil, huma.Error500InternalServerError("failed to query telemetry", err)
		}

		records := make([]TelemetryRecord, len(page.Records))
		for i, r := range page.Records {
			records[i] = TelemetryRecord{
				ID:          r.ID,
				ProcessedAt: r.ProcessedAt,
				MetricName:  r.MetricName,
				GpuID:       r.GpuID,
				Device:      r.Device,
				UUID:        r.UUID,
				ModelName:   r.ModelName,
				Hostname:    r.Hostname,
				Value:       r.Value,
				Container:   r.Container,
				Pod:         r.Pod,
				Namespace:   r.Namespace,
			}
		}

		out := &GetTelemetryOutput{}
		out.Body.GPUUUID = input.ID
		out.Body.Records = records
		out.Body.NextCursor = page.NextCursor
		out.Body.Total = page.Total
		return out, nil
	})
}
