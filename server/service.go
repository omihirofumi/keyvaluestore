package main

import (
	"context"
	pb "github.com/omihirofumi/keyvaluestore/keyvaluestore"
	"google.golang.org/grpc"
	"log"
	"net"
)

type server struct {
	pb.UnimplementedKeyValueServer
}

func (s *server) Get(ctx context.Context, r *pb.GetRequest) (*pb.GetResponse, error) {
	log.Printf("Received GET key=%v", r.Key)

	value, err := Get(r.Key)

	return &pb.GetResponse{Value: value}, err
}

func (s *server) Put(ctx context.Context, r *pb.PutRequest) (*pb.PutResponse, error) {
	log.Printf("Received PUT key=%v value=%v", r.Key, r.Value)

	err := Put(r.Key, r.Value)

	return &pb.PutResponse{}, err
}

func main() {
	s := grpc.NewServer()
	pb.RegisterKeyValueServer(s, &server{})

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}

}
