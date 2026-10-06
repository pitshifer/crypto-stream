package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
)

type Config struct {
	BinanceWsHost string         `json:"binance_ws_host"`
	Symbols       []SymbolConfig `json:"symbols"`
	LogLevel      slog.Level     `json:"log_level"`
	LogFormat     string         `json:"log_format"`

	GrpcAddr string `json:"grpc_addr"`
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

	if errs := cfg.validate(); len(errs) > 0 {
		return nil, errors.Join(errs...)
	}

	return &cfg, nil
}

func (c *Config) validate() []error {
	var ResultErrors []error

	if c.BinanceWsHost == "" {
		ResultErrors = append(ResultErrors, errors.New("binance_ws_host is required"))
	}

	if len(c.Symbols) == 0 {
		ResultErrors = append(ResultErrors, errors.New("symbols list is empty"))
	}

	if c.GrpcAddr == "" {
		ResultErrors = append(ResultErrors, errors.New("grpc_addr is required"))
	}

	symbolsMap := make(map[string]struct{})
	for i, sc := range c.Symbols {
		if sc.Symbol == "" {
			ResultErrors = append(ResultErrors, fmt.Errorf("symbols[%d][symbol] cannot be empty", i))
		}

		if sc.VolatilityThreshold <= 0 {
			ResultErrors = append(ResultErrors, fmt.Errorf("symbols[%d][volatility_threshold] (%s) cannot be zero or negative", i, sc.Symbol))
		}

		if _, ok := symbolsMap[sc.Symbol]; ok {
			ResultErrors = append(ResultErrors, fmt.Errorf("symbols[%d][symbol] (%s) is duplicated", i, sc.Symbol))
		}
		symbolsMap[sc.Symbol] = struct{}{}
	}

	return ResultErrors
}

func (c *Config) GetSymbols() []string {
	symbols := make([]string, len(c.Symbols))
	for i, s := range c.Symbols {
		symbols[i] = s.Symbol
	}
	return symbols
}


func  unusedBadlyFormatted( ) {}
