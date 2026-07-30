package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"

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
	host  string
	conns map[string]*websocket.Conn
	feed  map[string]chan TradeEvent

	mu sync.Mutex
}

func NewClient(host string) *Client {
	return &Client{
		host:  host,
		conns: make(map[string]*websocket.Conn),
		feed:  make(map[string]chan TradeEvent),
	}
}

func (c *Client) connect(symbol string) error {
	if conn, ok := c.conns[symbol]; ok && conn != nil {
		_ = c.Close(symbol)
	}

	url := fmt.Sprintf("wss://%s/ws/%s@trade", c.host, symbol)
	conn, resp, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		if resp != nil {
			log.Printf("handshake failed, status: %s", resp.Status)
		}
		return err
	}

	log.Println("connected, listening for trades...")

	c.mu.Lock()
	c.conns[symbol] = conn
	c.feed[symbol] = make(chan TradeEvent, 100)
	c.mu.Unlock()

	return nil
}

func (c *Client) Close(symbol string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conns[symbol] != nil {
		return c.conns[symbol].Close()
	}
	return nil
}

func (c *Client) Listen(ctx context.Context, symbol string) (<-chan TradeEvent, error) {
	var conn *websocket.Conn
	var feed chan TradeEvent

	c.mu.Lock()
	conn, ok := c.conns[symbol]
	c.mu.Unlock()

	if !ok || conn == nil {
		err := c.connect(symbol)
		if err != nil {
			return nil, err
		}
	}

	c.mu.Lock()
	conn = c.conns[symbol]
	feed = c.feed[symbol]
	c.mu.Unlock()

	var tradeEvent TradeEvent

	go func() {
		for {
			_, msg, err := conn.ReadMessage()
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
			case feed <- tradeEvent:
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		<-ctx.Done()
		if err := c.Close(symbol); err != nil {
			log.Println("error closing connection:", err)
		}
		close(feed)
	}()

	return feed, nil
}
