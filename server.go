package main

import (
	"errors"
	"flag"
	"fmt"
	"net"
	pb "pubsub"
	"sync"
	"math/rand"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
)

var (
	port = flag.Int("port", 10000, "The server port")
)


type ClientData struct {
	identity *pb.Identity
	stream pb.Subscriber_PullServer
	stop chan struct{}
	mutex   sync.Mutex
	connected bool
}

type ClientRegistry struct {
	mutex   sync.Mutex
	clients []*ClientData
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

func generateRandomString(size int)(string) {
	b := make([]rune, size)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func (this *ClientRegistry) getClient(identity *pb.Identity) (*ClientData, error){
	this.mutex.Lock()
	for _, client := range this.clients {
		if (client.identity.Name == identity.Name) {
			this.mutex.Unlock()
			return client, nil
		}		
	}
	this.mutex.Unlock()
	return nil, errors.New("Client was not registered")
}

func (this *ClientRegistry) Register(identity *pb.Identity) {
	this.mutex.Lock()
	newClient := new(ClientData)
	newClient.identity = identity
	newClient.stop = make(chan struct{})
	newClient.connected = false
	this.clients = append(this.clients, newClient)
	this.mutex.Unlock()
}

func (this *ClientRegistry) isRegistered(identity *pb.Identity)(bool) {
	this.mutex.Lock()
	for _, client := range this.clients {
		if (client.identity.Name == identity.Name) {
			this.mutex.Unlock()
			return true
		}		
	}
	this.mutex.Unlock()
	return false
}

func (this *ClientRegistry) Unregister(identity *pb.Identity) error {
	this.mutex.Lock()
	for index, client := range this.clients {
		if (client.identity.Name == identity.Name) {
			this.clients = append(this.clients[:index], this.clients[index+1:]...)
			this.mutex.Unlock()
			return nil
		}
	}
	this.mutex.Unlock()
	return errors.New("Client was not registered")
}

type SubscriptionRegistry struct {
	topics map[string]*MessageTopic
	mutex   sync.Mutex
}

func (this *SubscriptionRegistry) getTopic(key string) (topic *MessageTopic) {
	this.mutex.Lock()
	if value, exist := this.topics[key]; exist {
		topic = value
	} else {
		topic = new(MessageTopic)
		this.topics[key] = topic
	}
	this.mutex.Unlock()
	return
}

type MessageTopic struct {
	subscriptions []*ClientData
	mutex   sync.Mutex
}

func (this *MessageTopic) Spread(message *pb.Message) {
	this.mutex.Lock()
	for _, client := range this.subscriptions {
		client.mutex.Lock()
		if (client.connected == true) {
			client.stream.Send(message)
		}
		client.mutex.Unlock()
	}
	this.mutex.Unlock()
}

func (this *MessageTopic) isSubscribed(client *ClientData) bool {
	this.mutex.Lock()
	for _, subscribedClient := range this.subscriptions {
		if subscribedClient == client {
			this.mutex.Unlock()
			return true
		}
	}
	this.mutex.Unlock()
	return false
}

func (this *MessageTopic) Subscribe(client *ClientData) {
	this.mutex.Lock()
	this.subscriptions = append(this.subscriptions, client)
	this.mutex.Unlock()
}

func (this *MessageTopic) Unsubscribe(client *ClientData) error {
	this.mutex.Lock()
	for index, subscribedClient := range this.subscriptions {
		if subscribedClient == client {
			this.subscriptions = append(this.subscriptions[:index], this.subscriptions[index+1:]...)
			this.mutex.Unlock()
			return nil
		}
	}
	this.mutex.Unlock()
	return errors.New("Client was not subscribed to this topic")
}

type ServerContext struct {
	ClientRegistry
	SubscriptionRegistry
}

func (this *ServerContext) Authenticate(ctx context.Context, identity *pb.Identity) (*pb.Identity, error) {
	if identity.Name == "" {
		identity.Name = generateRandomString(8)
	}
	grpclog.Printf("Authenticating %s", identity.Name)
	if this.isRegistered(identity) == false {
		this.Register(identity)
		grpclog.Printf("Created new user %s", identity.Name)
	} else {
		grpclog.Printf("Existing user %s", identity.Name)
	}
	return identity, nil
}

func (this *ServerContext) Subscribe(ctx context.Context, request *pb.SubscribeRequest) (*pb.Subscription, error) {
	client, err := this.getClient(request.Identity)
	if err != nil {
		return nil, errors.New("Client was not authenticated")
	}

	grpclog.Printf("Subscribing %s to %s", request.Identity.Name, request.Subscription.Key)

	topic := this.getTopic(request.Subscription.Key)
	if (topic.isSubscribed(client) == true) {
		grpclog.Printf("Error already subscribed %s", request.Identity.Name)
		return nil, errors.New("Client already subscribed to this key")
	}
	topic.Subscribe(client)
	return request.Subscription, nil
}

func (this *ServerContext) Unsubscribe(ctx context.Context, request *pb.SubscribeRequest) (*pb.Subscription, error) {
	client, err := this.getClient(request.Identity)
	if err != nil {
		return nil, errors.New("Client was not authenticated")
	}

	topic := this.getTopic(request.Subscription.Key)
	if (topic.isSubscribed(client) == false) {
		grpclog.Printf("Error not subscribed %s", request.Identity.Name)
		return nil, errors.New("Client was not subscribed to this key")
	}
	topic.Unsubscribe(client)
	return request.Subscription, nil
}

func (this *ServerContext) Pull(identity *pb.Identity, stream pb.Subscriber_PullServer) error {
	client, err := this.getClient(identity)
	if err != nil {
		return errors.New("Client was not authenticated")
	}

	grpclog.Printf("Opening stream for %s", identity.Name)

	client.mutex.Lock()
	client.stream = stream
	client.connected = true
	client.mutex.Unlock()

	<-client.stop
//	for {
//		time.Sleep(1000000)
//	}

	return nil
}

func (this *ServerContext) Publish(ctx context.Context, request *pb.PublishRequest) (*pb.PublishResponse, error) {
	topic := this.getTopic(request.Key)
	for _, message := range request.Messages {
		topic.Spread(message)
	}
	return &pb.PublishResponse{}, nil
}

func newServer() *ServerContext {
	s := new(ServerContext)
	s.topics = make(map[string]*MessageTopic)
	return s
}

func main() {
	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		grpclog.Fatalf("failed to listen: %v", err)
	}
	var opts []grpc.ServerOption

	grpcServer := grpc.NewServer(opts...)
	context := newServer()
	pb.RegisterSubscriberServer(grpcServer, context)
	pb.RegisterPublisherServer(grpcServer, context)
	grpcServer.Serve(lis)
}
