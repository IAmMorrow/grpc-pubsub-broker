package main

import (
	"net"
	"fmt"
	pb "github.com/weackd/grpc-pubsub-broker/protobuf"
	sublib "github.com/weackd/grpc-pubsub-broker/subscriber/sublib"
	publib "github.com/weackd/grpc-pubsub-broker/publisher/publib"
	"google.golang.org/grpc"
	"time"
)

func startServer() {
	lis, errserv := net.Listen("tcp", fmt.Sprintf(":%d", 10000))
	if errserv != nil {
		fmt.Print("error serv")
		return
	}

	grpcServer := grpc.NewServer()
	context := newServerContext()
	pb.RegisterSubscriberServer(grpcServer, context)
	pb.RegisterPublisherServer(grpcServer, context)
	grpcServer.Serve(lis)		
}

func main() {
	go startServer()
	time.Sleep(3 * time.Second)

 	
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())

        conn, err := grpc.Dial("127.0.0.1:10000", opts...)
	if err != nil {
		fmt.Print("error sub")
		return
	}
	ctx := sublib.NewSubscriberContext()
	ctx.Client = pb.NewSubscriberClient(conn)

	ctx.Authenticate("")
	ctx.Subscribe("Tennis")

	connpub, _ := grpc.Dial("127.0.0.1:10000", opts...)
	publisher := pb.NewPublisherClient(connpub)
	for true {
		publib.Publish(publisher, "Tennis",  &pb.Message{Data: []byte("TEST")})
		time.Sleep(1 * time.Second)
	}

}
