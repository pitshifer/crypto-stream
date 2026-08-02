package main

import (
	"context"
	"log"
	"log/slog"
	"math"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/pitshifer/crypto-stream/internal/aggregator"
	"github.com/pitshifer/crypto-stream/internal/binance"
	"github.com/pitshifer/crypto-stream/internal/config"
)

func main() {
	cfg, err := config.NewConfig("config.json")
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	// logger
	var logHandler slog.Handler
	switch cfg.LogFormat {
	case "json":
		logHandler = slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: cfg.LogLevel,
		})
	default:
		logHandler = slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: cfg.LogLevel,
		})
	}
	slog.SetDefault(slog.New(logHandler))

	// pprofiler
	go func() {
		slog.Info("pprof listening on localhost:6060")
		if err := http.ListenAndServe("localhost:6060", nil); err != nil {
			slog.Error("pprof listen error", "error", err)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	wg := sync.WaitGroup{}
	wg.Add(len(cfg.Symbols))

	client := binance.NewClient(cfg.BinanceWsHost)

	for _, symbol := range cfg.Symbols {
		go func() {
			defer wg.Done()

			eventCh, err := client.Listen(ctx, symbol)
			if err != nil {
				slog.Error("listen error", "symbol", symbol, "error", err)
				return
			}

			aggregator := aggregator.NewAggregator(5 * time.Minute)
			ticker := time.NewTicker(3 * time.Second)
			defer ticker.Stop()

			for {
				select {
				case <-ticker.C:
					volatility := aggregator.Volatility()
					roundedVolatility := math.Round(volatility*100) / 100
					slog.Info("volatility", "symbol", symbol, "volatility", roundedVolatility)

				case event := <-eventCh:
					price, err := strconv.ParseFloat(event.Price, 64)
					if err != nil {
						slog.Error("parse price error", "symbol", symbol, "error", err)
						continue
					}
					aggregator.AddPrice(price, time.UnixMilli(event.TradeTime))

					slog.Debug("trade event", "symbol", symbol, "event", event)

				case <-ctx.Done():
					return
				}
			}
		}()
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		slog.Info("all goroutines finished")
	case <-time.After(5 * time.Second):
		slog.Warn("shutdown timer exceeded, forcing exit")
	}
}
