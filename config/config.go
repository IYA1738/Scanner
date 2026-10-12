package config

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/ethereum/go-ethereum/common"
)

type ScannerConfig struct {
	WorkersNum  int              `json:"workers_num"`
	BlockTimeMS int              `json:"block_time_ms"`
	GroupSize   uint             `json:"group_size"`
	Constants   ScannerConstants `json:"constants"`
}

type ScannerConstants struct {
	UniV3Factory     common.Address `json:"uni_v3_factory"`
	UniV4PoolManager common.Address `json:"uni_v4_poolmanager"`
}

type RPCConfig struct {
	URL           []string `json:"http_url"`
	WS_URL        []string `json:"ws_url"`
	RPCTimeout    int      `json:"rpc_timeout"`
	RetryCount    int      `json:"retry_count"`
	RetryInterval int      `json:"retry_interval"`
}

type ChainConfig struct {
	ChainID    int           `json:"chain_id"`
	RPCCfg     RPCConfig     `json:"rpc_config"`
	ScannerCfg ScannerConfig `json:"scanner_config"`
}

type LogConfig struct {
	Level  string `json:"level"`
	Format string `json:"format"`
}

type Config struct {
	Chains   map[string]ChainConfig `json:"chains"`
	Logger   LogConfig              `json:"logger"`
	Database DatabaseConfig         `json:"database"`
	TG       TGConfig               `json:"tg"`
}

type TGConfig struct {
	BotToken  string           `json:"bot_token"`
	ChatID    map[string]int64 `json:"chat_id"`
	TGTimeout int              `json:"tg_timeout"`
	TGActive  bool             `json:"tg_avtive"`
}

type DatabaseConfig struct {
	DBName   string `json:"dbname"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("cannot find config.json")
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("cannot parse config.json")
	}
	return &cfg, nil
}
