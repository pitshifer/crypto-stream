package grpcserver

import (
	"context"
	"slices"

	apiv1 "github.com/pitshifer/crypto-stream/internal/gen/api/v1"
	"github.com/pitshifer/crypto-stream/internal/quote"
	"github.com/pitshifer/crypto-stream/internal/volatility"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	apiv1.UnimplementedStreamerServiceServer
	symbols     []string
	volStorage  *volatility.Storage
	broadcaster *quote.Broadcaster
	ctx         context.Context
}

func NewServer(ctx context.Context, symbols []string, volStorage *volatility.Storage, broadcaster *quote.Broadcaster) *Server {
	return &Server{
		symbols:     symbols,
		volStorage:  volStorage,
		broadcaster: broadcaster,
		ctx:         ctx,
	}
}

func (s *Server) GetSymbols(context.Context, *apiv1.GetSymbolsRequest) (*apiv1.GetSymbolsResponse, error) {
	return &apiv1.GetSymbolsResponse{
		Symbols: s.symbols,
	}, nil
}

func (s *Server) GetVolatility(ctx context.Context, req *apiv1.GetVolatilityRequest) (*apiv1.GetVolatilityResponse, error) {
	symbol := req.GetSymbol()
	volatility, ok := s.volStorage.Get(symbol)
	if !ok {
		return nil, status.Errorf(codes.NotFound, "symbol %q not found", symbol)
	}

	return &apiv1.GetVolatilityResponse{
		Volatility: volatility,
		Symbol:     symbol,
	}, nil
}

func (s *Server) Quote(req *apiv1.QuoteRequest, stream apiv1.StreamerService_QuoteServer) error {
	symbol := req.GetSymbol()
	if !slices.Contains(s.symbols, symbol) {
		return status.Errorf(codes.NotFound, "symbol %q not found", symbol)
	}

	ch, cancel := s.broadcaster.Subscribe(symbol)
	defer cancel()

	for {
		select {
		case q := <-ch:
			err := stream.Send(&apiv1.QuoteResponse{
				Symbol:    symbol,
				Price:     q.Price,
				TradeTime: q.TradeTime.Unix(),
			})
			if err != nil {
				return err
			}

		case <-stream.Context().Done():
			return nil

		case <-s.ctx.Done():
			return nil
		}
	}
}
