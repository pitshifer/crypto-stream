package main

import (
	"context"
	"log"
	"strconv"
	"sync"
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

	ctx, cancel := context.WithTimeout(context.Background(), 360*time.Second)
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

			aggregator := aggregator.NewAggregator(5 * time.Minute)
			ticker := time.NewTicker(3 * time.Second)
			defer ticker.Stop()

			for {
				select {
				case <-ticker.C:
					volatility := aggregator.Volatility()
					log.Printf("symbol: %s, volatility: %.2f%%\n", symbol, volatility)

				case event := <-eventCh:
					price, err := strconv.ParseFloat(event.Price, 64)
					if err != nil {
						log.Printf("symbol: %s, parse price error: %v\n", symbol, err)
						continue
					}
					aggregator.AddPrice(price, time.UnixMilli(event.TradeTime))

					log.Printf("symbol: %s, TradeEvent: %+v\n", symbol, event)

				case <-ctx.Done():
					return
				}
			}
		}()
	}

	wg.Wait()
}
