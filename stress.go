package main

//import "fmt"
//import "io/ioutil"
import (
	"os/exec"
	"flag"
	"math/rand"
	"strconv"
	"os"
	"bufio"
	"fmt"
	"io/ioutil"
)

var (
	publisherNb = flag.Int("pubquantity", 10, "The quantity of parallel publishers")
	subscriberNb = flag.Int("subquantity", 0, "The quantity of parallel subscribers")
//	topicNb = flag.Int("topicquantity", 1, "The quantity of different topics")
	topicLenght = flag.Int("topiclenght", 10, "The lenght of the topic strings")
	msgFrequency = flag.Int("msgfrequency", 100000, "The nanoseconds between each message")
)

func generateRandomBytes(size int)([]byte) {
	token := make([]byte, size)
	rand.Read(token)
	return token
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

func generateRandomString(size int)(string) {
	b := make([]rune, size)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func main() {
	flag.Parse()

	var messages []string
	var topics []string
	var publishers []*exec.Cmd
	var subscribers []*exec.Cmd

	for _, value := range flag.Args() {
		size, err := strconv.Atoi(value)
		if (err != nil) {
			return
		}
		messages = append(messages, generateRandomString(size))
	}

	server := exec.Command("go", "run", "server.go")
	pipe, _ := server.StdoutPipe()
	server.Start()
	
	for i := 0; i < *publisherNb; i++ {
		topic := generateRandomString(int(*topicLenght))
		topics = append(topics, topic)
//		publisher := exec.Command("go", "run", "publisher.go", "--topic", topic, "--frequency", string(*msgFrequency), string(messages[0]))
		publisher := exec.Command("go", "run", "publisher.go", "--topic", "Tennis", "SALUT")
		publisher.Start()
		publishers = append(publishers, publisher)
	}
	subargs := []string{"run", "subscriber.go"}
	subargs = append(subargs, topics...)
	fmt.Printf("topics: %v\n", topics)
	fmt.Printf("messages: %v\n", messages)
	
	for i := 0; i < *subscriberNb; i++ {
		subscriber := exec.Command("go", subargs...)
		subscriber.Start()
		subscribers = append(subscribers, subscriber)
	}

	// server.Wait()
	for body, err := ioutil.ReadAll(pipe); err != nil; {
		fmt.Printf("server output: %s\n", body)
	}

	
	fmt.Print("Press 'Enter' to continue...")
	bufio.NewReader(os.Stdin).ReadBytes('\n') 
//	publisher := exec.Command("go", "run", "publisher.go", "--topic=Volley",  "--frequency=1000000", "lol", "lil", "lul")
//	output, _ := publisher.Run()
//	fmt.Printf("Output is %s\n", output)
/*	dateOut, err := dateCmd.Output()
	if err != nil {
		panic(err)
	}
	fmt.Println("> date")
	fmt.Println(string(dateOut))
*/

//	grepCmd := exec.Command("grep", "hello")
}	
