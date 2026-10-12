package tg

import (
	"context"
	"fmt"
	"strings"
)

type TGEvent uint8

const (
	TGEventNewPool TGEvent = iota
	TGEventAddLiquidity
	TGEventTransfer
)

func (t *TGPublisher) PublishToTG(ctx context.Context, chatID int64, tgEvent TGEvent, args ...any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var title string
	switch tgEvent {
	case TGEventNewPool:
		title = "新池子"
	case TGEventAddLiquidity:
		title = "添加流动性"
	case TGEventTransfer:
		title = "转账"
	default:
		return fmt.Errorf("unsupported TG event: %d", tgEvent)
	}
	text := title
	if body := strings.TrimSpace(fmt.Sprintln(args...)); body != "" {
		text += "\n" + body
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
