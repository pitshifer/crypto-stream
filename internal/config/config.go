package config

import (
	"encoding/json"
	"os"
)

type Config struct {
	BinanceWsHost string   `json:"binance_ws_host"`
	Symbols       []string `json:"symbols"`
}

func NewConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}
