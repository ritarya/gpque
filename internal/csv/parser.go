package csv

import (
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"strings"

	"gpqueue/internal/model"
)

// Parser maps CSV rows to partial TelemetryRecord values.
// ProcessedAt, ID, and StreamerID are set by the caller.
type Parser struct {
	r       *csv.Reader
	headers map[string]int
}

// New creates a Parser and consumes the header row.
func New(r io.Reader) (*Parser, error) {
	cr := csv.NewReader(r)
	cr.TrimLeadingSpace = true

	headers, err := cr.Read()
	if err != nil {
		return nil, fmt.Errorf("read header: %w", err)
	}

	idx := make(map[string]int, len(headers))
	for i, h := range headers {
		idx[strings.TrimSpace(h)] = i
	}

	return &Parser{r: cr, headers: idx}, nil
}

// Next reads the next CSV row and returns the corresponding TelemetryRecord.
// Returns io.EOF when the file is exhausted.
func (p *Parser) Next() (*model.TelemetryRecord, error) {
	row, err := p.r.Read()
	if err != nil {
		return nil, err
	}
	return p.parse(row)
}

func (p *Parser) parse(row []string) (*model.TelemetryRecord, error) {
	get := func(col string) string {
		i, ok := p.headers[col]
		if !ok || i >= len(row) {
			return ""
		}
		return strings.TrimSpace(row[i])
	}

	nullable := func(col string) *string {
		v := get(col)
		if v == "" {
			return nil
		}
		return &v
	}

	gpuID, err := strconv.Atoi(get("gpu_id"))
	if err != nil {
		return nil, fmt.Errorf("invalid gpu_id %q: %w", get("gpu_id"), err)
	}

	val, err := strconv.ParseFloat(get("value"), 64)
	if err != nil {
		return nil, fmt.Errorf("invalid value %q: %w", get("value"), err)
	}

	return &model.TelemetryRecord{
		SourceTimestamp: get("timestamp"),
		MetricName:      get("metric_name"),
		GpuID:           gpuID,
		Device:          get("device"),
		UUID:            get("uuid"),
		ModelName:       get("modelName"),
		Hostname:        get("Hostname"),
		Container:       nullable("container"),
		Pod:             nullable("pod"),
		Namespace:       nullable("namespace"),
		Value:           val,
		LabelsRaw:       get("labels_raw"),
	}, nil
}
