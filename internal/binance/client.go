package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/gorilla/websocket"
)

type TradeEvent struct {
	EventType string `json:"e"`
	EventTime int64  `json:"E"`
	Symbol    string `json:"s"`
	TradeID   int64  `json:"t"`
	Price     string `json:"p"`
	Quantity  string `json:"q"`
	TradeTime int64  `json:"T"`
}

type Client struct {
	url  string
	conn *websocket.Conn
}

func NewClient(host, symbol string) *Client {
	return &Client{
		url: fmt.Sprintf("wss://%s/ws/%s@trade", host, symbol),
	}
}

func (c *Client) Connect() error {
	if c.conn != nil {
		_ = c.Close()
	}

	conn, resp, err := websocket.DefaultDialer.Dial(c.url, nil)
	if err != nil {
		if resp != nil {
			log.Printf("handshake failed, status: %s", resp.Status)
		}
		return err
	}

	log.Println("connected, listening for trades...")
	c.conn = conn

	return nil
}

func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

func (c *Client) Stream(ctx context.Context) (<-chan TradeEvent, error) {
	if c.conn == nil {
		if err := c.Connect(); err != nil {
			return nil, err
		}
	}

	events := make(chan TradeEvent)
	var tradeEvent TradeEvent

	go func() {
		for {
			_, msg, err := c.conn.ReadMessage()
			if err != nil {
				if ctx.Err() != nil {
					return // context canceled, exit the goroutine
				}
				log.Println("read error:", err)
				return
			}

			if err := json.Unmarshal(msg, &tradeEvent); err != nil {
				log.Println("unmarshal error:", err)
				continue
			}

			select {
			case events <- tradeEvent:
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		<-ctx.Done()
		if err := c.Close(); err != nil {
			log.Println("error closing connection:", err)
		}
		close(events)
	}()

	return events, nil
}

func Listen(ctx context.Context, host, symbol string, handle func(TradeEvent)) error {
	client := NewClient(host, symbol)

	events, err := client.Stream(ctx)
	if err != nil {
		log.Println("stream error:", err)
		return err
	}

	for event := range events {
		handle(event)
	}

	return nil
}
