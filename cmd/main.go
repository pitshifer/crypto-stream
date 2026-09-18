package main

import (
	"context"
	"log"
	"log/slog"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/pitshifer/crypto-stream/internal/binance"
	"github.com/pitshifer/crypto-stream/internal/config"
	apiv1 "github.com/pitshifer/crypto-stream/internal/gen/api/v1"
	"github.com/pitshifer/crypto-stream/internal/grpcserver"
	"github.com/pitshifer/crypto-stream/internal/quote"
	"google.golang.org/grpc"
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
	broadcaster := quote.NewBroadcaster()

	lis, err := net.Listen("tcp", cfg.GrpcAddr)
	if err != nil {
		slog.Error("failed to listen gRPC", "error", err)
		os.Exit(1)
	}

	grpcSrv := grpc.NewServer()
	apiv1.RegisterStreamerServiceServer(grpcSrv, grpcserver.NewServer(ctx, cfg.GetSymbols(), broadcaster))

	go func() {
		slog.Info("gRPC server listening on", "address", cfg.GrpcAddr)
		if err := grpcSrv.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			slog.Error("gRPC server error", "error", err)
		}
	}()

	for _, symbolCfg := range cfg.Symbols {
		go func() {
			defer wg.Done()

			eventCh, err := client.Listen(ctx, symbolCfg.Symbol)
			if err != nil {
				slog.Error("listen error", "symbol", symbolCfg.Symbol, "error", err)
				return
			}

			for {
				select {
				case event := <-eventCh:
					price, err := strconv.ParseFloat(event.Price, 64)
					if err != nil {
						slog.Error("parse price error", "symbol", symbolCfg.Symbol, "error", err)
						continue
					}
					quantity, err := strconv.ParseFloat(event.Quantity, 64)
					if err != nil {
						slog.Error("parse quantity error", "symbol", symbolCfg.Symbol, "error", err)
						continue
					}
					broadcaster.Publish(symbolCfg.Symbol, quote.Quote{
						Price:     price,
						TradeTime: time.UnixMilli(event.TradeTime),
					})

					slog.Debug("trade event",
						"symbol", symbolCfg.Symbol,
						"quantity", quantity,
						"trade", event,
					)

				case <-ctx.Done():
					return
				}
			}
		}()
	}

	<-ctx.Done()
	slog.Info("shutting down...")

	grpcSrv.GracefulStop()

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
