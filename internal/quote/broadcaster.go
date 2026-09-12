package quote

import (
	"sync"
	"time"
)

const subscriberBufferSize = 20

type Broadcaster struct {
	mu   sync.RWMutex
	subs map[string]map[chan Quote]struct{}
}

type Quote struct {
	Price     float64
	TradeTime time.Time
}

func NewBroadcaster() *Broadcaster {
	return &Broadcaster{
		subs: make(map[string]map[chan Quote]struct{}),
	}
}

func (b *Broadcaster) Subscribe(symbol string) (<-chan Quote, func()) {
	ch := make(chan Quote, subscriberBufferSize)

	b.mu.Lock()
	if _, ok := b.subs[symbol]; !ok {
		b.subs[symbol] = make(map[chan Quote]struct{})
	}
	b.subs[symbol][ch] = struct{}{}
	b.mu.Unlock()

	cancel := func() {
		b.mu.Lock()
		defer b.mu.Unlock()

		if _, ok := b.subs[symbol][ch]; ok {
			delete(b.subs[symbol], ch)
			close(ch)
		}
	}

	return ch, cancel
}

func (b *Broadcaster) Publish(symbol string, quote Quote) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	for ch := range b.subs[symbol] {
		select {
		case ch <- quote:
		default:
			// If the channel is full, we can choose to drop the quote or handle it differently.
			// For now, we'll just drop it to avoid blocking.
		}
	}
}
