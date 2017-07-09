package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"sync"
	"time"

	pb "pubsub"

	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
)

var (
	serverAddr         = flag.String("server_addr", "127.0.0.1:10000", "The server address in the format of host:port")
	serverHostOverride = flag.String("server_host_override", "x.test.youtube.com", "The server name use to verify the hostname returned by TLS handshake")
)

type SubscriberContext struct {
	client   pb.SubscriberClient
	identity *pb.Identity
	speed    int
	size     int
	mutex    sync.Mutex
}

func (s *SubscriberContext) Authenticate(name string) error {
	grpclog.Printf("Authenticating")
	if identity, error := s.client.Authenticate(context.Background(), &pb.Identity{Name: name}); error == nil {
		grpclog.Printf("Success")

		s.identity = identity
		return nil
	}
	return nil
}

func (s *SubscriberContext) Subscribe(key string) error {
	grpclog.Printf("Subscribing to %s", key)

	request := &pb.SubscribeRequest{Identity: s.identity, Subscription: &pb.Subscription{Key: key}}
	if _, error := s.client.Subscribe(context.Background(), request); error == nil {
		grpclog.Printf("Success")

		return nil
	}
	return nil
}

func (s *SubscriberContext) Pull() error {
	grpclog.Printf("Opening message stream")

	if stream, error := s.client.Pull(context.Background(), s.identity); error == nil {
		grpclog.Printf("Success")

		for {
			msg, err := stream.Recv()
			if err == io.EOF {
				grpclog.Printf("end of file")
				break
			}
			if err != nil {
				grpclog.Fatalf(err.Error())
			}
			s.mutex.Lock()
			s.speed++
			s.size += len(msg.Data)
			s.mutex.Unlock()
			//grpclog.Println(msg)
		}

		return nil
	}
	return nil
}

func NewSubscriberContext() *SubscriberContext {
	s := new(SubscriberContext)
	s.speed = 0
	return s
}

func update(ctx *SubscriberContext) {
	for {
		time.Sleep(1 * time.Second)
		ctx.mutex.Lock()
		grpclog.Printf("Receiving %d message/second. Average message size %f kb/s. Received %f kb/s", ctx.speed, float64(ctx.size)/float64(ctx.speed)/1000.0, float64(ctx.size)/1000.0)
		ctx.speed = 0
		ctx.size = 0
		ctx.mutex.Unlock()
	}
}

func main() {
	flag.Parse()
	var opts []grpc.DialOption

	opts = append(opts, grpc.WithInsecure())

	conn, err := grpc.Dial(*serverAddr, opts...)
	if err != nil {
		grpclog.Fatalf("fail to dial: %v", err)
	}

	defer conn.Close()

	ctx := NewSubscriberContext()
	ctx.client = pb.NewSubscriberClient(conn)

	// Looking for a valid feature
	ctx.Authenticate("")

	for _, value := range flag.Args() {
		ctx.Subscribe(value)
	}

	go func(msg string) {
		fmt.Println(msg)
	}("going")
	go update(ctx)
	ctx.Pull()

}
