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

			client := binance.NewClient("wss://stream.binance.com:9443/ws/" + s + "@trade")
			if err := client.Connect(); err != nil {
				log.Println("connection error:", err)
				return
			}
			defer client.Close()

			events, err := client.Stream(ctx)
			if err != nil {
				log.Println("stream error:", err)
				return
			}

			for event := range events {
				log.Printf("Trade Event: %+v\n", event)
			}
		}(symbol)
	}

	wg.Wait()
}
