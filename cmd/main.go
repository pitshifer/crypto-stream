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
	config, err := config.NewConfig("config.json")
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	wg := sync.WaitGroup{}
	wg.Add(len(config.Symbols))

	for _, symbol := range config.Symbols {
		go func(s string) {
			defer wg.Done()

			if err := binance.Listen(ctx, config.BinanceWsHost, s, func(event binance.TradeEvent) {
				log.Printf("TradeEvent: %+v\n", event)
			}); err != nil {
				log.Printf("symbol: %s, listen error: %v\n", s, err)
			}
		}(symbol)
	}

	wg.Wait()
}
