package main

import (
	"flag"
	"strconv"
)

var (
	port = flag.Int("port", 10000, "The server port")
)


func main() {
	flag.Parse()
	pbserv := newPubSubServer()
	pbserv.Start(strconv.Itoa(*port))
}
