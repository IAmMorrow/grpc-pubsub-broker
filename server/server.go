package main

import (
	"fmt"
	"net"
	"errors"
	pb "github.com/weackd/grpc-pubsub-broker/protobuf"
	"sync"
	"math/rand"
	"golang.org/x/net/context"
	"google.golang.org/grpc/grpclog"	
	"google.golang.org/grpc"
)

type ClientData struct {
	identity *pb.Identity
	channel chan *pb.Message
	mutex   sync.Mutex
	connected bool
}

type ClientRegistry struct {
	mutex   sync.Mutex
	clients []*ClientData
}

func (this *ServerContext) Stop() {
	for _, client := range this.clients {		
		client.mutex.Lock()
		close(client.channel)
		client.connected = false
		client.mutex.Unlock()
	}
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
	newClient.channel = make(chan *pb.Message)
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
			client.channel <- message
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
	client.connected = true
	client.mutex.Unlock()
	
	for msg := range client.channel {
		if err := stream.Send(msg); err != nil {
			return err
		}
		
	}

	client.mutex.Lock()
	client.connected = false
	client.mutex.Unlock()

	grpclog.Printf("Closing stream for %s", identity.Name)

	
	return nil
}

func (this *ServerContext) Publish(ctx context.Context, request *pb.PublishRequest) (*pb.PublishResponse, error) {
	topic := this.getTopic(request.Key)
	for _, message := range request.Messages {
		topic.Spread(message)
	}
	return &pb.PublishResponse{}, nil
}

type PubSubServer struct {
	context	*ServerContext
	server	*grpc.Server
}

func (this *PubSubServer) Stop() {
	this.context.Stop()
	this.server.GracefulStop()
}

func (this *PubSubServer) Start(port string) {
	var opts []grpc.ServerOption

	lis, err := net.Listen("tcp", ":" + port)
	if err != nil {
		fmt.Print(err.Error())
		return
	}
	this.server = grpc.NewServer(opts...)
	pb.RegisterSubscriberServer(this.server, this.context)
	pb.RegisterPublisherServer(this.server, this.context)
	this.server.Serve(lis)
}

func newPubSubServer() *PubSubServer {
	s := new(PubSubServer)
	s.context = newServerContext()
	return s
}

func newServerContext() *ServerContext {
	s := new(ServerContext)
	s.topics = make(map[string]*MessageTopic)
	return s
}
