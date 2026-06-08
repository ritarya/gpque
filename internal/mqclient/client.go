package mqclient

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"gpqueue/internal/model"
)

// QueueClient is the interface used by streamers and collectors to communicate
// with the message queue service.
type QueueClient interface {
	Publish(ctx context.Context, topic string, payload []byte) error
	Fetch(ctx context.Context, topic, groupID string, limit int) ([]model.Message, error)
	Commit(ctx context.Context, topic, groupID string, offset int64) error
	Nack(ctx context.Context, topic, groupID string, offset int64) error
	Close() error
}

// Client is the HTTP implementation of QueueClient.
type Client struct {
	base       string
	producerID string
	http       *http.Client
}

// New returns a Client that talks to the queue service at baseURL and
// identifies itself as producerID in publish requests.
func New(baseURL, producerID string) *Client {
	return &Client{
		base:       baseURL,
		producerID: producerID,
		http:       &http.Client{Timeout: 10 * time.Second},
	}
}

// Publish encodes payload as base64 and posts it to /topics/{topic}/messages.
func (c *Client) Publish(ctx context.Context, topic string, payload []byte) error {
	body, err := json.Marshal(struct {
		Payload    string `json:"payload"`
		ProducerID string `json:"producer_id"`
	}{
		Payload:    base64.StdEncoding.EncodeToString(payload),
		ProducerID: c.producerID,
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		fmt.Sprintf("%s/topics/%s/messages", c.base, topic),
		bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()

	if resp.StatusCode >= 300 {
		return fmt.Errorf("publish: queue returned HTTP %d", resp.StatusCode)
	}
	return nil
}

type fetchResponse struct {
	Messages []struct {
		Offset      int64     `json:"offset"`
		Payload     string    `json:"payload"`
		PublishedAt time.Time `json:"published_at"`
		ProducerID  string    `json:"producer_id"`
	} `json:"messages"`
	NextOffset int64 `json:"next_offset"`
}

// Fetch long-polls for up to limit messages from the given consumer group.
func (c *Client) Fetch(ctx context.Context, topic, groupID string, limit int) ([]model.Message, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		fmt.Sprintf("%s/topics/%s/messages?group=%s&limit=%d", c.base, topic, groupID, limit),
		nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("fetch: queue returned HTTP %d", resp.StatusCode)
	}

	var fr fetchResponse
	if err := json.NewDecoder(resp.Body).Decode(&fr); err != nil {
		return nil, fmt.Errorf("decode fetch response: %w", err)
	}

	msgs := make([]model.Message, 0, len(fr.Messages))
	for _, m := range fr.Messages {
		payload, err := base64.StdEncoding.DecodeString(m.Payload)
		if err != nil {
			return nil, fmt.Errorf("decode payload at offset %d: %w", m.Offset, err)
		}
		msgs = append(msgs, model.Message{
			Offset:      m.Offset,
			Payload:     payload,
			PublishedAt: m.PublishedAt,
			ProducerID:  m.ProducerID,
		})
	}
	return msgs, nil
}

// Commit advances the consumer group offset to offset.
func (c *Client) Commit(ctx context.Context, topic, groupID string, offset int64) error {
	return c.offsetOp(ctx, topic, "commit", groupID, offset)
}

// Nack signals that offset could not be processed; triggers retry or DLQ routing.
func (c *Client) Nack(ctx context.Context, topic, groupID string, offset int64) error {
	return c.offsetOp(ctx, topic, "nack", groupID, offset)
}

func (c *Client) offsetOp(ctx context.Context, topic, op, groupID string, offset int64) error {
	body, err := json.Marshal(struct {
		GroupID string `json:"group_id"`
		Offset  int64  `json:"offset"`
	}{GroupID: groupID, Offset: offset})
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		fmt.Sprintf("%s/topics/%s/%s", c.base, topic, op),
		bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()

	if resp.StatusCode >= 300 {
		return fmt.Errorf("%s: queue returned HTTP %d", op, resp.StatusCode)
	}
	return nil
}

// Close is a no-op; the http.Client has no persistent connections to release.
func (c *Client) Close() error { return nil }
