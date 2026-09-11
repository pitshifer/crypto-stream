package grpcserver

import (
	"context"

	apiv1 "github.com/pitshifer/crypto-stream/internal/gen/api/v1"
	"github.com/pitshifer/crypto-stream/internal/volatility"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	apiv1.UnimplementedStreamerServiceServer
	symbols    []string
	volStorage *volatility.Storage
}

func NewServer(symbols []string, volStorage *volatility.Storage) *Server {
	return &Server{
		symbols:    symbols,
		volStorage: volStorage,
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
