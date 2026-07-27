package main

import (
	"context"
	"log"
	"time"

	"github.com/pitshifer/crypto-stream/internal/binance"
)

func main() {
	client := binance.NewClient("wss://stream.binance.com:9443/ws/btcusdt@trade")
	if err := client.Connect(); err != nil {
		log.Fatal("connection error:", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	events, err := client.Stream(ctx)
	if err != nil {
		log.Fatal("stream error:", err)
	}

	for event := range events {
		log.Printf("Trade Event: %+v\n", event)
	}
}
