package subscriber

import (
	"context"
	"io"
	"sync"
	pb "github.com/weackd/grpc-pubsub-broker/protobuf"
	"google.golang.org/grpc/grpclog"
)

type SubscriberContext struct {
	Client   pb.SubscriberClient
	identity *pb.Identity
	Speed    int
	Size     int
	Mutex    sync.Mutex
}

func (s *SubscriberContext) Authenticate(name string) error {
	grpclog.Printf("Authenticating")
	if identity, error := s.Client.Authenticate(context.Background(), &pb.Identity{Name: name}); error == nil {
		grpclog.Printf("Success")

		s.identity = identity
		return nil
	}
	return nil
}

func (s *SubscriberContext) Subscribe(key string) error {
	grpclog.Printf("Subscribing to %s", key)

	request := &pb.SubscribeRequest{Identity: s.identity, Subscription: &pb.Subscription{Key: key}}
	if _, error := s.Client.Subscribe(context.Background(), request); error == nil {
		grpclog.Printf("Success")

		return nil
	}
	return nil
}

func (s *SubscriberContext) Pull() error {
	grpclog.Printf("Opening message stream")

	if stream, error := s.Client.Pull(context.Background(), s.identity); error == nil {
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
			s.Mutex.Lock()
			s.Speed++
			s.Size += len(msg.Data)
			s.Mutex.Unlock()
		}

		return nil
	}
	return nil
}

func NewSubscriberContext() *SubscriberContext {
	s := new(SubscriberContext)
	s.Speed = 0
	return s
}

