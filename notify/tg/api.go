package tg

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"unicode/utf8"
)

type APIError struct {
	StatusCode  int
	ErrorCode   int
	Description string
	RetryAfter  int
}

func (e *APIError) Error() string {
	message := fmt.Sprintf("telegram sendMessage failed (HTTP %d, code %d): %s", e.StatusCode, e.ErrorCode, e.Description)
	if e.RetryAfter > 0 {
		message += fmt.Sprintf(" (retry after %d seconds)", e.RetryAfter)
	}
	return message
}

func (t *TGPublisher) SendMessage(ctx context.Context, chatID int64, text string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if chatID == 0 {
		return fmt.Errorf("tg chat_id must not be zero")
	}
	if !utf8.ValidString(text) || strings.TrimSpace(text) == "" || utf8.RuneCountInString(text) > 4096 {
		return fmt.Errorf("tg message must contain 1-4096 characters of valid UTF-8 text")
	}
	payload, err := json.Marshal(struct {
		ChatID int64  `json:"chat_id"`
		Text   string `json:"text"`
	}{ChatID: chatID, Text: text})
	if err != nil {
		return fmt.Errorf("encode telegram message: %w", err)
	}
	endpoint := "https://api.telegram.org/bot" + t.cfg.BotToken + "/sendMessage"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("create telegram request: %s", strings.ReplaceAll(err.Error(), t.cfg.BotToken, "[redacted]"))
	}
	req.Header.Set("Content-Type", "application/json")
	response, err := t.client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var requestErr *url.Error
		for errors.As(err, &requestErr) {
			err = requestErr.Err
		}
		return fmt.Errorf("telegram sendMessage request: %w", err)
	}
	defer response.Body.Close()
	var result struct {
		OK          bool   `json:"ok"`
		ErrorCode   int    `json:"error_code"`
		Description string `json:"description"`
		Parameters  struct {
			RetryAfter int `json:"retry_after"`
		} `json:"parameters"`
	}
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&result); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("telegram sendMessage HTTP %d: invalid JSON response", response.StatusCode)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 || !result.OK {
		return &APIError{
			StatusCode: response.StatusCode, ErrorCode: result.ErrorCode,
			Description: strings.ReplaceAll(result.Description, t.cfg.BotToken, "[redacted]"),
			RetryAfter:  result.Parameters.RetryAfter,
		}
	}
	return nil
}
