package tg

import (
	"fmt"
	"math"
	"net/http"
	"strings"
	"time"

	"example.com/scanner_demo/config"
)

type TGPublisher struct {
	cfg    config.TGConfig
	client *http.Client
}

func NewTGPublisher(cfg config.TGConfig) (*TGPublisher, error) {
	chatIDs := make(map[string]int64, len(cfg.ChatID))
	for name, id := range cfg.ChatID {
		chatIDs[name] = id
	}
	cfg.ChatID = chatIDs
	publisher := &TGPublisher{cfg: cfg}
	if cfg.BotToken == "" || strings.ContainsAny(cfg.BotToken, "/?#\\ \t\r\n") {
		return nil, fmt.Errorf("invalid tg bot_token")
	}
	if cfg.TGTimeout <= 0 || uint64(cfg.TGTimeout) > uint64(math.MaxInt64)/uint64(time.Second) {
		return nil, fmt.Errorf("tg_timeout must be positive and fit in time.Duration")
	}
	publisher.client = &http.Client{
		Timeout: time.Duration(cfg.TGTimeout) * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	return publisher, nil
}
