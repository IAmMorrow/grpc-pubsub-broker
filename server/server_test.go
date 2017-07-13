package main

import (
	"context"
	"testing"
	"io"
	"net"
	"fmt"
//	"time"
	pb "github.com/weackd/grpc-pubsub-broker/protobuf"

	"google.golang.org/grpc"
)

var sent	int = 0
var received	int = 0


type Subscriber struct {
	client pb.SubscriberClient
	identity *pb.Identity
}

func (this *Subscriber) Pull() {
	stream, err := this.client.Pull(context.Background(), this.identity)	
	if err != nil {
		fmt.Print("ERROR PULL SUB")
	}
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			fmt.Printf("end of file")
			break
		}
		if err != nil {
			fmt.Printf(err.Error())
			break
		}
		received += 1		
	}
}

func (this *Subscriber) Authenticate() {
	identity, err := this.client.Authenticate(context.Background(), &pb.Identity{Name: ""})
	if err != nil {
		fmt.Print("ERROR AUTHENTICATE SUB")
	}
	this.identity = identity
}

func (this *Subscriber) Subscribe(key string) {
	request := &pb.SubscribeRequest{Identity: this.identity, Subscription: &pb.Subscription{Key: key}}

	_, err := this.client.Subscribe(context.Background(), request)
	if err != nil {
		fmt.Print("ERROR SUBSCRIBE SUB")
	}
}

type Publisher struct {
	client		pb.PublisherClient
	keys		[]string
	messages	[]string
}

func (this *Publisher) Publish(key string, msg string) {
	request := &pb.PublishRequest{Key: key, Messages: []*pb.Message{&pb.Message{Data: []byte(msg)}}}
	_, err := this.client.Publish(context.Background(), request)
	if err != nil {
		fmt.Print("ERROR PUBLISH PUB")
	}
	sent += 1
}

type TestContext struct {
	subscribers	[]Subscriber
	publishers	[]Publisher
	server		*grpc.Server
	serverPort	string
}

func (this *TestContext) StartServer(port string) {
	lis, err := net.Listen("tcp", port)
	fmt.Printf("OPEN PORT %s", port)

	if err != nil {
		fmt.Print("CANT OPEN SERVER SOCKET")
	}

	this.server = grpc.NewServer()
	context := newServerContext()
	pb.RegisterSubscriberServer(this.server, context)
	pb.RegisterPublisherServer(this.server, context)
	this.serverPort = port
	go this.server.Serve(lis)

}

func (this *TestContext) AddPub(keys []string, messages []string) {
	var opts []grpc.DialOption
	var pub Publisher

	opts = append(opts, grpc.WithInsecure())
	
	conn, err := grpc.Dial(this.serverPort, opts...)
	if err != nil {
		fmt.Print("CANT CONNECT CLIENT PUB SOCKET")
	}
	pub.client = pb.NewPublisherClient(conn)
	pub.keys = keys
	pub.messages = messages	
	this.publishers = append(this.publishers, pub)
}

func (this *TestContext) AddSub(keys []string) {
	var opts []grpc.DialOption
	var sub Subscriber

	opts = append(opts, grpc.WithInsecure())
	
	conn, err := grpc.Dial(this.serverPort, opts...)
	if err != nil {
		fmt.Print("CANT CONNECT CLIENT SUB SOCKET")
	}
	sub.client = pb.NewSubscriberClient(conn)
	sub.Authenticate()
	for _, key := range keys {
		sub.Subscribe(key)
	}
	this.subscribers = append(this.subscribers, sub)
}

func BenchmarkServer(b *testing.B) {
	var test TestContext

	test.StartServer(":10000")

	topics := []string{"test", "test2"}
	test.AddSub(topics)
	test.AddPub(topics, []string{"0000000", "111111111111111", "2222222222222222222222222222222"})

	for _, sub := range test.subscribers {
		go sub.Pull()
	}


	b.ResetTimer()
	
	for received < 100000 {
		for _, pub := range test.publishers {
			pub.Publish(pub.keys[0], pub.messages[0])
		}
	}	
}
