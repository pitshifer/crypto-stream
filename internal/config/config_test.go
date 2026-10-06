package config_test

import (
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pitshifer/crypto-stream/internal/config"
)

// writeConfig создаёт временный файл конфигурации и возвращает путь к нему.
// t.TempDir() сам удаляется после теста, поэтому чистить за собой не нужно.
func writeConfig(t *testing.T, content string) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("failed to write test config: %v", err)
	}

	return path
}

func TestNewConfig_RejectsInvalidConfig(t *testing.T) {
	tests := []struct {
		name string
		// content — содержимое config.json для этого случая.
		content string
		// wantErrContains — подстрока, которая обязана быть в тексте ошибки,
		// чтобы по логу деплоя было понятно, какое поле виновато.
		// Пустая строка означает «достаточно самого факта ошибки».
		wantErrContains string
	}{
		{
			name:            "empty object",
			content:         `{}`,
			wantErrContains: "",
		},
		{
			name: "empty symbols list",
			content: `{
				"binance_ws_host": "stream.binance.com:9443",
				"symbols": [],
				"log_level": "info",
				"log_format": "json",
				"grpc_addr": ":50051"
			}`,
			wantErrContains: "symbols",
		},
		{
			name: "symbol with empty name",
			content: `{
				"binance_ws_host": "stream.binance.com:9443",
				"symbols": [
					{"symbol": "", "volatility_threshold": 0.4}
				],
				"log_level": "info",
				"log_format": "json",
				"grpc_addr": ":50051"
			}`,
			wantErrContains: "symbols[0]",
		},
		{
			name: "zero volatility threshold",
			content: `{
				"binance_ws_host": "stream.binance.com:9443",
				"symbols": [
					{"symbol": "btcusdt", "volatility_threshold": 0}
				],
				"log_level": "info",
				"log_format": "json",
				"grpc_addr": ":50051"
			}`,
			wantErrContains: "volatility_threshold",
		},
		{
			name: "negative volatility threshold",
			content: `{
				"binance_ws_host": "stream.binance.com:9443",
				"symbols": [
					{"symbol": "btcusdt", "volatility_threshold": -1}
				],
				"log_level": "info",
				"log_format": "json",
				"grpc_addr": ":50051"
			}`,
			wantErrContains: "volatility_threshold",
		},
		{
			name: "missing grpc_addr",
			content: `{
				"binance_ws_host": "stream.binance.com:9443",
				"symbols": [
					{"symbol": "btcusdt", "volatility_threshold": 0.4}
				],
				"log_level": "info",
				"log_format": "json"
			}`,
			wantErrContains: "grpc_addr",
		},
		{
			name: "missing binance_ws_host",
			content: `{
				"symbols": [
					{"symbol": "btcusdt", "volatility_threshold": 0.4}
				],
				"log_level": "info",
				"log_format": "json",
				"grpc_addr": ":50051"
			}`,
			wantErrContains: "binance_ws_host",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := config.NewConfig(writeConfig(t, tt.content))
			if err == nil {
				t.Fatalf("expected an error, got valid config %+v", cfg)
			}

			if tt.wantErrContains != "" && !strings.Contains(err.Error(), tt.wantErrContains) {
				t.Errorf("error %q does not mention %q", err, tt.wantErrContains)
			}
		})
	}
}

// Этот тест проходит и до появления валидации — он нужен не как RED, а как
// страховка от слишком строгих проверок: валидный конфиг должен оставаться
// валидным, а поля — разбираться в ожидаемые значения.
func TestNewConfig_AcceptsValidConfig(t *testing.T) {
	path := writeConfig(t, `{
		"binance_ws_host": "stream.binance.com:9443",
		"symbols": [
			{"symbol": "btcusdt", "volatility_threshold": 0.4},
			{"symbol": "ethusdt", "volatility_threshold": 1}
		],
		"log_level": "info",
		"log_format": "json",
		"grpc_addr": ":50051"
	}`)

	cfg, err := config.NewConfig(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.BinanceWsHost != "stream.binance.com:9443" {
		t.Errorf("BinanceWsHost = %q, want %q", cfg.BinanceWsHost, "stream.binance.com:9443")
	}
	if cfg.GrpcAddr != ":50051" {
		t.Errorf("GrpcAddr = %q, want %q", cfg.GrpcAddr, ":50051")
	}
	if cfg.LogLevel != slog.LevelInfo {
		t.Errorf("LogLevel = %v, want %v", cfg.LogLevel, slog.LevelInfo)
	}
	if cfg.LogFormat != "json" {
		t.Errorf("LogFormat = %q, want %q", cfg.LogFormat, "json")
	}

	if len(cfg.Symbols) != 2 {
		t.Fatalf("len(Symbols) = %d, want 2", len(cfg.Symbols))
	}
	if cfg.Symbols[0].Symbol != "btcusdt" || cfg.Symbols[0].VolatilityThreshold != 0.4 {
		t.Errorf("Symbols[0] = %+v, want {btcusdt 0.4}", cfg.Symbols[0])
	}

	if got := cfg.GetSymbols(); len(got) != 2 || got[0] != "btcusdt" || got[1] != "ethusdt" {
		t.Errorf("GetSymbols() = %v, want [btcusdt ethusdt]", got)
	}
}
