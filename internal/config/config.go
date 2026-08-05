package config

import (
	"encoding/json"
	"log/slog"
	"os"
)

type Config struct {
	BinanceWsHost string         `json:"binance_ws_host"`
	Symbols       []SymbolConfig `json:"symbols"`
	LogLevel      slog.Level     `json:"log_level"`
	LogFormat     string         `json:"log_format"`

	KafkaBrokers    []string `json:"kafka_brokers"`
	KafkaAlertTopic string   `json:"kafka_alert_topic"`
}

type SymbolConfig struct {
	Symbol              string  `json:"symbol"`
	VolatilityThreshold float64 `json:"volatility_threshold"`
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
