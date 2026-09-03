package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	reconnectMinDelay = 1 * time.Second
	reconnectMaxDelay = 30 * time.Second
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

func (t TradeEvent) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("eventType", t.EventType),
		slog.String("symbol", t.Symbol),
		slog.String("price", t.Price),
		slog.String("quantity", t.Quantity),
		slog.Int64("tradeTime", t.TradeTime),
	)
}

type Client struct {
	host  string
	conns map[string]*websocket.Conn

	mu sync.Mutex
}

func NewClient(host string) *Client {
	return &Client{
		host:  host,
		conns: make(map[string]*websocket.Conn),
	}
}

func (c *Client) dial(ctx context.Context, symbol string) (*websocket.Conn, error) {
	url := fmt.Sprintf("wss://%s/ws/%s@trade", c.host, symbol)
	conn, resp, err := websocket.DefaultDialer.DialContext(ctx, url, nil)
	if err != nil {
		if resp != nil {
			slog.Error("handshake failed", "status", resp.Status, "symbol", symbol, "error", err)
		}
		return nil, err
	}

	slog.Info("connected, listening for trades", "symbol", symbol)
	return conn, nil
}

func (c *Client) setConnect(symbol string, conn *websocket.Conn) {
	c.mu.Lock()
	c.conns[symbol] = conn
	c.mu.Unlock()
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
	conn, err := c.dial(ctx, symbol)
	if err != nil {
		return nil, err
	}
	c.setConnect(symbol, conn)

	feed := make(chan TradeEvent, 100)

	go func() {
		<-ctx.Done()
		if err := c.Close(symbol); err != nil {
			slog.Error("error closing connection", "error", err)
		}
	}()

	go c.readLoop(ctx, symbol, conn, feed)

	return feed, nil
}

func (c *Client) readLoop(ctx context.Context, symbol string, conn *websocket.Conn, feed chan<- TradeEvent) {
	defer close(feed)

	for {
		err := c.readMessages(ctx, conn, feed)
		if ctx.Err() != nil {
			return
		}
		slog.Warn("read loop error", "symbol", symbol, "error", err)

		newConn, ok := c.reconnect(ctx, symbol, reconnectMinDelay)
		if !ok {
			return
		}
		conn = newConn
	}
}

func (c *Client) readMessages(ctx context.Context, conn *websocket.Conn, feed chan<- TradeEvent) error {
	var tradeEvent TradeEvent

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			slog.Error("read error", "error", err)
			return err
		}

		if err := json.Unmarshal(msg, &tradeEvent); err != nil {
			slog.Error("unmarshal error", "error", err)
			continue
		}

		select {
		case feed <- tradeEvent:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (c *Client) reconnect(ctx context.Context, symbol string, delay time.Duration) (*websocket.Conn, bool) {
	for {
		select {
		case <-ctx.Done():
			return nil, false
		case <-time.After(delay):
		}

		conn, err := c.dial(ctx, symbol)
		if err != nil {
			slog.Error("reconnect failed", "symbol", symbol, "error", err)
			delay = delay * 2
			delay = min(delay, reconnectMaxDelay)
			continue
		}

		c.setConnect(symbol, conn)
		return conn, true
	}
}
