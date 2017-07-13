package main

import (
	"flag"
	"net"
	"fmt"
	pb "github.com/weackd/grpc-pubsub-broker/protobuf"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
)

var (
	port = flag.Int("port", 10000, "The server port")
)


func main() {
	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		grpclog.Fatalf("failed to listen: %v", err)
	}
	var opts []grpc.ServerOption

	grpcServer := grpc.NewServer(opts...)
	context := newServerContext()
	pb.RegisterSubscriberServer(grpcServer, context)
	pb.RegisterPublisherServer(grpcServer, context)
	grpcServer.Serve(lis)
}
