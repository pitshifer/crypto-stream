package volatility

import (
	"strings"
	"sync"
)

type Storage struct {
	volatilities map[string]float64
	mu           sync.Mutex
}

func NewStorage() *Storage {
	return &Storage{
		volatilities: make(map[string]float64),
	}
}

func (s *Storage) Set(symbol string, volatility float64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	sl := strings.ToLower(symbol)
	s.volatilities[sl] = volatility
}

func (s *Storage) Get(symbol string) (float64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	sl := strings.ToLower(symbol)
	if volatility, ok := s.volatilities[sl]; ok {
		return volatility, true
	}
	return 0, false
}
