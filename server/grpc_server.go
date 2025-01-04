package server

import (
	"fmt"
	"log"
	"net"

	"github.com/Bl4ck-h00d/stashdb/core/store"
	"github.com/Bl4ck-h00d/stashdb/protobuf"
	"github.com/Bl4ck-h00d/stashdb/raft"
	"google.golang.org/grpc"
)

type GRPCServer struct {
	grpcAddress string
	server      *grpc.Server
	service     *GRPCService
	listener    net.Listener
}

func NewGRPCServer(grpcAddress, storageEngine, dataPath, certificateFile, commonName string, raftServer *raft.RaftServer) (*GRPCServer, error) {

	server := grpc.NewServer()
	listener, err := net.Listen("tcp", grpcAddress)

	dataStore, err := store.NewStore(storageEngine, dataPath)
	if err != nil {
		panic(fmt.Sprintf("failed to open database: %v", err))
	}

	service := NewGRPCService(dataStore, certificateFile,commonName,raftServer)

	protobuf.RegisterStashDBServiceServer(server, service)

	return &GRPCServer{
		grpcAddress: grpcAddress,
		server:      server,
		service:     service,
		listener:    listener,
	}, nil
}

func (s *GRPCServer) Start() error {

	go func() {
		_ = s.server.Serve(s.listener)
	}()

	log.Printf("gRPC server started @%v", s.grpcAddress)
	return nil
}

func (s *GRPCServer) Stop() error {
	s.server.Stop()
	s.listener.Close()

	log.Println("gRPC server stopped")
	return nil
}
