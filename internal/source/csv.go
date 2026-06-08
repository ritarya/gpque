package source

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/google/uuid"

	csvparser "gpqueue/internal/csv"
	"gpqueue/internal/model"
)

// CSVSource implements Source by reading rows from a CSV file in an infinite loop.
type CSVSource struct {
	path       string
	streamerID string
	file       *os.File
	parser     *csvparser.Parser
}

// NewCSV opens the CSV file at path and returns a ready CSVSource.
func NewCSV(path, streamerID string) (*CSVSource, error) {
	s := &CSVSource{path: path, streamerID: streamerID}
	if err := s.openFile(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *CSVSource) openFile() error {
	f, err := os.Open(s.path)
	if err != nil {
		return fmt.Errorf("open csv %q: %w", s.path, err)
	}
	p, err := csvparser.New(f)
	if err != nil {
		f.Close()
		return err
	}
	s.file = f
	s.parser = p
	return nil
}

// Next blocks until the next record is ready, then returns it with a fresh
// ID, ProcessedAt wall-clock time, and StreamerID set.
// When the file is exhausted it loops back to the first data row.
func (s *CSVSource) Next(ctx context.Context) (*model.TelemetryRecord, error) {
	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		rec, err := s.parser.Next()
		if err == io.EOF {
			s.file.Close()
			if err := s.openFile(); err != nil {
				return nil, err
			}
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("parse csv: %w", err)
		}

		rec.ID = uuid.New().String()
		rec.ProcessedAt = time.Now()
		rec.StreamerID = s.streamerID
		return rec, nil
	}
}

// Close releases the underlying file handle.
func (s *CSVSource) Close() error {
	if s.file != nil {
		return s.file.Close()
	}
	return nil
}
