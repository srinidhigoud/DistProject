package main

import (
	"flag"
	"fmt"
	"log"
	rand "math/rand"
	"net"
	"os"
	"time"

	"google.golang.org/grpc"

	"github.com/nyu-distributed-systems-fa18/DistProject/pb"
	"github.com/nyu-distributed-systems-fa18/DistProject/util"
	// "DistProject/pb"
	// "DistProject/util"
)

func main() {
	// Argument parsing
	var r *rand.Rand
	var seed int64
	var peers util.ArrayPeers
	// var clients util.ArrayClients
	var clientPort int
	var pbftPort int
	flag.Int64Var(&seed, "seed", -1,
		"Seed for random number generator, values less than 0 result in use of time")
	flag.IntVar(&clientPort, "port", 3000,
		"Port on which server should listen to client requests")
	flag.IntVar(&pbftPort, "pbft", 3001,
		"Port on which server should listen to Pbft requests")
	flag.Var(&peers, "peer", "A peer for this process")
	// flag.Var(&clients, "client", "A client for this process")
	flag.Parse()
	// clients.Set("127.0.0.1:3008")
	// Initialize the random number generator
	if seed < 0 {
		r = rand.New(rand.NewSource(time.Now().UnixNano()))
	} else {
		r = rand.New(rand.NewSource(seed))
	}

	// Get hostname
	name, err := os.Hostname()
	if err != nil {
		// Without a host name we can't really get an ID, so die.
		log.Fatalf("Could not get hostname")
	}

	id := fmt.Sprintf("%s:%d", name, pbftPort)
	log.Printf("Starting peer with ID %s", id)

	// Convert port to a string form
	portString := fmt.Sprintf(":%d", clientPort)
	// Create socket that listens on the supplied port
	c, err := net.Listen("tcp", portString)
	if err != nil {
		// Note the use of Fatalf which will exit the program after reporting the error.
		log.Fatalf("Could not create listening socket %v", err)
	}
	// Create a new GRPC server
	s := grpc.NewServer()

	// Initialize KVStore
	store := KVStore{C: make(chan InputChannelType), store: make(map[string]string)}
	go serve(&store, r, &peers, id, pbftPort, true)

	// Tell GRPC that s will be serving requests for the KvStore service and should use store (defined on line 23)
	// as the struct whose methods should be called in Response.
	pb.RegisterKvStoreServer(s, &store)
	log.Printf("Going to listen on port %v", clientPort)
	// Start serving, this will block this function and only return when done.
	if err := s.Serve(c); err != nil {
		log.Fatalf("Failed to serve %v", err)
	}
	log.Printf("Done listening")
}
