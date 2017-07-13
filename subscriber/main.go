package main

import (
	"time"
	"flag"
	"fmt"
	pb "github.com/weackd/grpc-pubsub-broker/protobuf"
	sublib "github.com/weackd/grpc-pubsub-broker/subscriber/sublib"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
)

var (
	serverAddr         = flag.String("server_addr", "127.0.0.1:10000", "The server address in the format of host:port")
	serverHostOverride = flag.String("server_host_override", "x.test.youtube.com", "The server name use to verify the hostname returned by TLS handshake")
)

func update(ctx *sublib.SubscriberContext) {
	for {
		time.Sleep(1 * time.Second)
		ctx.Mutex.Lock()
		grpclog.Printf("Receiving %d message/second. Average message size %f kb/s. Received %f kb/s", ctx.Speed, float64(ctx.Size)/float64(ctx.Speed)/1000.0, float64(ctx.Size)/1000.0)
		ctx.Speed = 0
		ctx.Size = 0
		ctx.Mutex.Unlock()
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

	ctx := sublib.NewSubscriberContext()
	ctx.Client = pb.NewSubscriberClient(conn)

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
