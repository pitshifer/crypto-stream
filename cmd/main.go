package main

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/pitshifer/crypto-stream/internal/binance"
	"github.com/pitshifer/crypto-stream/internal/config"
)

func main() {
	cfg, err := config.NewConfig("config.json")
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	wg := sync.WaitGroup{}
	wg.Add(len(cfg.Symbols))

	client := binance.NewClient(cfg.BinanceWsHost)

	for _, symbol := range cfg.Symbols {
		go func() {
			defer wg.Done()

			eventCh, err := client.Listen(ctx, symbol)
			if err != nil {
				log.Printf("symbol: %s, listen error: %v\n", symbol, err)
				return
			}

			for event := range eventCh {
				log.Printf("symbol: %s, TradeEvent: %+v\n", symbol, event)
			}
		}()
	}

	wg.Wait()
}
