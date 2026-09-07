package grpcserver

import (
	"context"

	apiv1 "github.com/pitshifer/crypto-stream/internal/gen/api/v1"
)

type Server struct {
	apiv1.UnimplementedStreamerServiceServer
	symbols []string
}

func NewServer(symbols []string) *Server {
	return &Server{
		symbols: symbols,
	}
}

func (s *Server) GetSymbols(context.Context, *apiv1.GetSymbolsRequest) (*apiv1.GetSymbolsResponse, error) {
	return &apiv1.GetSymbolsResponse{
		Symbols: s.symbols,
	}, nil
}
