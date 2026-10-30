package tg

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"html"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"example.com/scanner_demo/scanner/internal/config"
	"example.com/scanner_demo/scanner/internal/domain"
)

type TGEvent uint8

const (
	TGEventNewV3Pool TGEvent = iota
	TGEventV3AddLiquidity
	TGEventV4AddLiquidity
	TGEventTransfer
)

type TGMsg struct {
	chatID int64
	text   string
}

type TGResponse struct {
	OK          bool   `json:"ok"`
	ErrorCode   int    `json:"error_code"`
	Description string `json:"description"`
}

type TGPublisher struct {
	cfg         config.TGConfig
	client      *http.Client
	queue       chan TGMsg
	log         *slog.Logger
	explorerURL string
}

func NewTGPublisher(cfg config.TGConfig, log *slog.Logger, explorerURL string) (*TGPublisher, error) {
	if cfg.TGActive == false {
		return nil, fmt.Errorf("TG service is off")
	}
	if cfg.BotToken == "" {
		return nil, fmt.Errorf("TG Bot token is nil")
	}

	client := &http.Client{
		Timeout: time.Duration(cfg.TGTimeout) * time.Second,
	}

	return &TGPublisher{
		cfg:         cfg,
		client:      client,
		queue:       make(chan TGMsg, 128),
		log:         log,
		explorerURL: explorerURL,
	}, nil
}

func (t *TGPublisher) SendMessage(ctx context.Context, chatID int64, text string) error {
	msg := TGMsg{
		chatID: chatID,
		text:   text,
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case t.queue <- msg:
		return nil
	default:
		return fmt.Errorf("queue is full")
	}
}

func (t *TGPublisher) ConsumeMessage(ctx context.Context) error {
	endpoint := "https://api.telegram.org/bot" + t.cfg.BotToken + "/sendMessage"
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg := <-t.queue:
			payload, err := json.Marshal(map[string]any{
				"chat_id":    msg.chatID,
				"text":       msg.text,
				"parse_mode": "HTML",
				"link_preview_options": map[string]any{
					"is_disabled": true,
				},
			})
			if err != nil {
				t.log.Error("encode tg message failed", "chat_id", msg.chatID, "error", err)
				continue
			}
			req, err := http.NewRequestWithContext(
				ctx,
				http.MethodPost,
				endpoint,
				bytes.NewReader(payload),
			)
			if err != nil {
				t.log.Error("create tg request failed", "chat_id", msg.chatID,
					"error", strings.ReplaceAll(err.Error(), t.cfg.BotToken, "[redacted]"))
				continue
			}
			req.Header.Set("Content-Type", "application/json")
			resp, err := t.client.Do(req)
			if err != nil {
				t.log.Error("send tg request failed", "chat_id", msg.chatID,
					"error", strings.ReplaceAll(err.Error(), t.cfg.BotToken, "[redacted]"))
				continue
			}
			var res TGResponse
			err = json.NewDecoder(resp.Body).Decode(&res)
			resp.Body.Close()
			if err != nil {
				t.log.Error("decode tg response failed", "chat_id", msg.chatID,
					"http_status", resp.StatusCode, "error", err)
				continue
			}
			if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices || !res.OK {
				t.log.Error("tg message rejected", "chat_id", msg.chatID,
					"http_status", resp.StatusCode, "error_code", res.ErrorCode,
					"description", strings.ReplaceAll(res.Description, t.cfg.BotToken, "[redacted]"))
				continue
			}
		}
	}
}

func (t *TGPublisher) PublishToTG(ctx context.Context, chatID int64, tgEvent TGEvent, args ...any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var text string
	switch tgEvent {
	case TGEventNewV3Pool:
		event, ok := args[0].(*domain.UniV3PoolCreate)
		if !ok {
			return fmt.Errorf("new V3 pool event requires *domain.UniV3PoolCreate")
		}
		text = t.buildNewV3PoolNotify(event)

	case TGEventV3AddLiquidity:

	case TGEventTransfer:

	default:
		return fmt.Errorf("unsupported TG event: %d", tgEvent)
	}

	return t.SendMessage(ctx, chatID, text)
}

func (t *TGPublisher) PublishToChat(ctx context.Context, chatName string, tgEvent TGEvent, args ...any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	chatID, ok := t.cfg.ChatID[chatName]
	if !ok {
		return fmt.Errorf("unknown tg chat name: %q", chatName)
	}
	return t.PublishToTG(ctx, chatID, tgEvent, args...)
}

func (t *TGPublisher) buildNewV3PoolNotify(event *domain.UniV3PoolCreate) string {
	fee := strconv.FormatFloat(
		float64(event.Fee)/10000, 'f', -1, 64,
	)
	poolURL := t.explorerURL + "/address/" + event.Pool.Hex()

	return fmt.Sprintf(
		"<b>V3池子创建</b>\n"+
			"<b>Token0:</b> <code>%s</code>\n"+
			"<b>Token1:</b> <code>%s</code>\n"+
			"手续费: <b>%s%%</b>\n"+
			"TickSpacing: %d\n"+
			"<b>池子地址</b>: <code>%s</code>\n"+
			"🔗<a href=\"%s\">查看池子</a>",
		event.Token0.Hex(),
		event.Token1.Hex(),
		fee,
		event.TickSpacing,
		event.Pool.Hex(),
		html.EscapeString(poolURL),
	)
}
