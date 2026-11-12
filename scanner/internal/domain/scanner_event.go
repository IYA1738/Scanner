package domain

type ScannerEvent struct {
	EventID   string `json:"event_id"`
	EventType string `json:"event_type"`
	ChainID   int64  `json:"chain_id"`
	Data      any    `json:"data"`
}
