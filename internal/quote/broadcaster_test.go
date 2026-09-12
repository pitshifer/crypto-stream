package quote_test

import (
	"sync"
	"testing"
	"time"

	"github.com/pitshifer/crypto-stream/internal/quote"
)

const testTimeout = 100 * time.Millisecond

func TestBroadcaster_DeliversPublishedQuoteToSubscriber(t *testing.T) {
	b := quote.NewBroadcaster()

	ch, cancel := b.Subscribe("BTCUSDT")
	defer cancel()

	want := quote.Quote{Price: 65000.5, TradeTime: time.Now()}
	b.Publish("BTCUSDT", want)

	select {
	case got := <-ch:
		if got != want {
			t.Fatalf("got %+v, want %+v", got, want)
		}
	case <-time.After(testTimeout):
		t.Fatal("timed out waiting for published quote")
	}
}

func TestBroadcaster_MultipleSubscribersReceiveSameQuote(t *testing.T) {
	b := quote.NewBroadcaster()

	ch1, cancel1 := b.Subscribe("ETHUSDT")
	defer cancel1()
	ch2, cancel2 := b.Subscribe("ETHUSDT")
	defer cancel2()

	want := quote.Quote{Price: 3200, TradeTime: time.Now()}
	b.Publish("ETHUSDT", want)

	for i, ch := range []<-chan quote.Quote{ch1, ch2} {
		select {
		case got := <-ch:
			if got != want {
				t.Fatalf("subscriber %d: got %+v, want %+v", i, got, want)
			}
		case <-time.After(testTimeout):
			t.Fatalf("subscriber %d: timed out waiting for published quote", i)
		}
	}
}

func TestBroadcaster_DoesNotDeliverToOtherSymbols(t *testing.T) {
	b := quote.NewBroadcaster()

	btcCh, cancelBTC := b.Subscribe("BTCUSDT")
	defer cancelBTC()
	ethCh, cancelETH := b.Subscribe("ETHUSDT")
	defer cancelETH()

	b.Publish("BTCUSDT", quote.Quote{Price: 65000, TradeTime: time.Now()})

	select {
	case <-btcCh:
	case <-time.After(testTimeout):
		t.Fatal("BTCUSDT subscriber did not receive its own quote")
	}

	select {
	case got := <-ethCh:
		t.Fatalf("ETHUSDT subscriber unexpectedly received %+v", got)
	case <-time.After(testTimeout):
		// Тишина и есть ожидаемый результат: паблишер не должен задевать
		// подписчиков другого символа.
	}
}

func TestBroadcaster_PublishWithoutSubscribersIsNoop(t *testing.T) {
	b := quote.NewBroadcaster()

	done := make(chan struct{})
	go func() {
		b.Publish("BTCUSDT", quote.Quote{Price: 1, TradeTime: time.Now()})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(testTimeout):
		t.Fatal("Publish blocked with no subscribers")
	}
}

func TestBroadcaster_PublishDoesNotBlockOnSlowSubscriber(t *testing.T) {
	b := quote.NewBroadcaster()

	_, cancel := b.Subscribe("BTCUSDT")
	defer cancel()

	// Никто не читает из канала — буфер подписчика неизбежно переполнится.
	// Publish не должен блокироваться на медленном подписчике: лишние
	// котировки для него просто дропаются.
	done := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			b.Publish("BTCUSDT", quote.Quote{Price: float64(i), TradeTime: time.Now()})
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(testTimeout):
		t.Fatal("Publish blocked on a slow subscriber instead of dropping the message")
	}
}

func TestBroadcaster_CancelStopsDeliveryAndClosesChannel(t *testing.T) {
	b := quote.NewBroadcaster()

	ch, cancel := b.Subscribe("BTCUSDT")
	cancel()

	b.Publish("BTCUSDT", quote.Quote{Price: 1, TradeTime: time.Now()})

	select {
	case got, ok := <-ch:
		if ok {
			t.Fatalf("expected channel to be closed after cancel, got value %+v", got)
		}
	case <-time.After(testTimeout):
		t.Fatal("channel was not closed after cancel")
	}
}

func TestBroadcaster_CancelIsIdempotent(t *testing.T) {
	b := quote.NewBroadcaster()

	_, cancel := b.Subscribe("BTCUSDT")

	cancel()
	cancel() // повторный вызов не должен паниковать
}

func TestBroadcaster_ConcurrentSubscribeAndPublish(t *testing.T) {
	b := quote.NewBroadcaster()

	var wg sync.WaitGroup

	// Несколько горутин публикуют...
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				b.Publish("BTCUSDT", quote.Quote{Price: float64(j), TradeTime: time.Now()})
			}
		}()
	}

	// ...пока другие горутины параллельно подписываются, читают и отписываются.
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ch, cancel := b.Subscribe("BTCUSDT")
			defer cancel()
			for {
				select {
				case <-ch:
				case <-time.After(10 * time.Millisecond):
					return
				}
			}
		}()
	}

	wg.Wait()
}
