package main

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/pitshifer/crypto-stream/internal/binance"
)

func main() {
	symbols := []string{"btcusdt", "ethusdt", "bnbusdt"}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	wg := sync.WaitGroup{}
	wg.Add(len(symbols))

	for _, symbol := range symbols {
		go func(s string) {
			defer wg.Done()

			if err := binance.Listen(ctx, s, func(event binance.TradeEvent) {
				log.Printf("TradeEvent: %+v\n", event)
			}); err != nil {
				log.Printf("listen error: %s %v\n", s, err)
			}
		}(symbol)
	}

	wg.Wait()
}
