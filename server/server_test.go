package main

import (
	"context"
	"testing"
	"io"
	"net"
	"fmt"
	"sync"
	pb "github.com/weackd/grpc-pubsub-broker/protobuf"
	"google.golang.org/grpc"
)

var sent	int = 0
var received	int = 0
var rmutex	sync.Mutex
var smutex	sync.Mutex


type Subscriber struct {
	client		pb.SubscriberClient
	identity	*pb.Identity
	conn		*grpc.ClientConn
}

func (this *Subscriber) Pull() error {
	stream, err := this.client.Pull(context.Background(), this.identity)	
	if err != nil {
		return err
	}
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		rmutex.Lock()
		received += 1
		rmutex.Unlock()
	}
	return nil
}

func (this *Subscriber) Authenticate() error {
	identity, err := this.client.Authenticate(context.Background(), &pb.Identity{Name: ""})
	if err != nil {
		return err
	}
	this.identity = identity
	return nil
}

func (this *Subscriber) Subscribe(key string) error {
	request := &pb.SubscribeRequest{Identity: this.identity, Subscription: &pb.Subscription{Key: key}}

	_, err := this.client.Subscribe(context.Background(), request)
	if err != nil {
		return err
	}
	return nil
}

type Publisher struct {
	client		pb.PublisherClient
	conn		*grpc.ClientConn
}

func (this *Publisher) Publish(key string, msg string) error {
	request := &pb.PublishRequest{Key: key, Messages: []*pb.Message{&pb.Message{Data: []byte(msg)}}}
	_, err := this.client.Publish(context.Background(), request)
	if err != nil {
		return err
	}
	sent += 1
	return nil
}

type TestContext struct {
	subscribers	[]Subscriber
	publishers	[]Publisher
	server		*grpc.Server
	serverPort	string
	serverContext   *ServerContext
}

func (this *TestContext) Stop() {
	this.serverContext.Stop()
	for _, pub := range this.publishers {
		pub.conn.Close()
	}
	for _, sub := range this.subscribers {
		sub.conn.Close()
	}
	this.server.GracefulStop()
}

func (this *TestContext) StartServer(port string) error {
	lis, err := net.Listen("tcp", port)
	fmt.Printf("OPEN PORT %s\n", port)
	this.serverPort = port

	if err != nil {
		return err
	}

	this.server = grpc.NewServer()
	context := newServerContext()
	this.serverContext = context
	pb.RegisterSubscriberServer(this.server, context)
	pb.RegisterPublisherServer(this.server, context)
	go this.server.Serve(lis)
	return nil
}

func (this *TestContext) AddPub(keys []string, messages []string) error {
	var opts []grpc.DialOption
	var pub Publisher

	opts = append(opts, grpc.WithInsecure())
	
	conn, err := grpc.Dial(this.serverPort, opts...)
	if err != nil {
		return err
	}
	pub.client = pb.NewPublisherClient(conn)
	pub.conn = conn
 	this.publishers = append(this.publishers, pub)
	return nil
}

func (this *TestContext) AddSub(keys []string) error {
	var opts []grpc.DialOption
	var sub Subscriber

	opts = append(opts, grpc.WithInsecure())
	
	conn, err := grpc.Dial(this.serverPort, opts...)
	if err != nil {
		return err
	}
	sub.client = pb.NewSubscriberClient(conn)
	sub.Authenticate()
	for _, key := range keys {
		err := sub.Subscribe(key)
		if err != nil {
			return err
		}
	}
	sub.conn = conn
	go sub.Pull()
	this.subscribers = append(this.subscribers, sub)
	return nil
}

func (this *TestContext) StressTest(b *testing.B, msgLimit int, subNb int, pubNb int, topics []string, messages []string) {

	for i := 0; i < subNb; i++ {
		if err := this.AddSub(topics); err != nil {
			b.Fatalf("Couldn't add subscriber")
		}
	}

	for i := 0; i < pubNb; i++ {
		if err := this.AddPub(topics, messages); err != nil {
			b.Fatalf("Couldn't add publisher")
		}
	}

	b.Logf("Starting Server benchmark, sending %d messages to %d subscribers using %d publishers", msgLimit, subNb, pubNb)
	b.ResetTimer()

	x := 0
	y := 0
	
	for {
		rmutex.Lock()
		receivedMessages := received
		rmutex.Unlock()

		if receivedMessages >= msgLimit * subNb {
			break
		}

		for _, pub := range this.publishers {
			smutex.Lock()
			if sent < msgLimit {
				pub.Publish(topics[x], messages[y])
			}
			smutex.Unlock()
			x = (x + 1) % len(topics)
			y = (y + 1) % len(messages)
		}
	}
	b.Logf("Received %d messages with %d subscribers", received, subNb)
}

func BenchmarkServer1p1s(b *testing.B) {
	sent = 0
	received = 0
	var test TestContext

	if err := test.StartServer(":35000"); err != nil {
		b.Fatalf("Couldn't start server: %s", err.Error())
	}

	topics := []string{
		"test",
		"test2"}
	
	messages := []string {
		"qwertyuiop",
		"Message",
		"6746468463846843684354"}

	test.StressTest(b, 5000, 1, 1, topics, messages)
	fmt.Printf("DONE !\n")
	
	test.Stop()
}

/*
func BenchmarkServer1p10s(b *testing.B) {
	sent = 0
	received = 0
	var test TestContext

	if err := test.StartServer(":35000"); err != nil {
		b.Fatalf("Couldn't start server: %s", err.Error())
	}

	topics := []string{
		"test",
		"test2"}
	
	messages := []string {
		"qwertyuiop",
		"Message",
		"6746468463846843684354"}

	test.StressTest(b, 20000, 10, 10, topics, messages)
	fmt.Printf("DONE !\n")
	test.Stop()
}

func BenchmarkServer40000_10p100s(b *testing.B) {
	sent = 0
	received = 0
	var test TestContext

	if err := test.StartServer(":35000"); err != nil {
		b.Fatalf("Couldn't start server: %s", err.Error())
	}

	topics := []string{
		"test",
		"test2"}
	
	messages := []string {
		"qwertyuiop",
		"Message",
		"6746468463846843684354"}

	test.StressTest(b, 40000, 100, 10, topics, messages)
	fmt.Printf("DONE !\n")
	test.Stop()
}

*/
