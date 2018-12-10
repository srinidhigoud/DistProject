package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	context "golang.org/x/net/context"
	// "google.golang.org/grpc"

	"github.com/nyu-distributed-systems-fa18/DistProject/pb"
	"github.com/nyu-distributed-systems-fa18/DistProject/util"
	//"context"
	"google.golang.org/grpc"
	//"DistProject/pb"
	//"DistProject/util"
)

type Validation struct {
	t	int64
	k	string
	v	string
}

func usage() {
	fmt.Printf("Usage %s <endpoint>\n", os.Args[0])
	flag.PrintDefaults()
}
//res.GetKv().Key, res.GetKv().Value

func compare(v1 Validation, v2 Validation) (bool) {
	if v1.t == v2.t {
		if v1.k == v2.k {
			if v1.v == v2.v{
				return true
			}
		}
	}
	return false
}

func acceptResult(mapS *map[int64]int64, mapV *map[int64]Validation, r *PbftLocal) (*pb.Result, error) {
	numberOfValidResponses := 0
	for numberOfValidResponses < 2 { //lot of changes required for a better performance
		select {
			case res := <-r.ResponseChan:
			if v, exists := mapS[res.sequenceID]; v < 2 {
				check := Validation{t: timestamp, k:nodeResult.Kv.Key, v:nodeResult.Kv.Value}
				if v > 0 {
					checkee := mapV[v-1]
					if compare(checkee, check) {
						mapS[res.sequenceID] = v+1
						numberOfValidResponses = v+1
						mapV[res.sequenceID] = check
					}
				} else {
					mapS[res.sequenceID] = v+1
					numberOfValidResponses = v+1
					mapV[res.sequenceID] = check
				}
				
			} 
		}
	return &res.nodeResult, nil
}

func main() {
	// Take endpoint as input
	mappedSeq := make(map[int64]int64)
	mappedVal := make(map[int64]Validation)
	var primary string
	var pbftPort int
	flag.IntVar(&pbftPort, "pbft", 3008,
		"Port on which server should listen to Pbft requests")
	flag.StringVar(&primary, "primary", "127.0.0.1:3001", 
		"Pbft Primary call")
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lshortfile)

	name, err := os.Hostname()
	if err != nil {
		log.Fatalf("Could not get hostname")
	}
	id := fmt.Sprintf("%s:%d", name, pbftPort)
	log.Printf("Starting the client with ID %s")
	log.Printf("Connecting to %v", primary)

	// Connect to the server. We use WithInsecure since we do not configure https in this class.
	conn, err := grpc.Dial(primary, grpc.WithInsecure())
	//Ensure connection did not fail.
	if err != nil {
		log.Fatalf("Failed to dial GRPC server %v", err)
	}
	log.Printf("Connected")
	// Create a KvStore client
	kvc := pb.NewKvStoreClient(conn)
	port := 3008
	pbft := util.PbftGlobal{ResponseChan: make(chan pb.ClientResponse)}
	go util.RunPbftGlobalServer(&pbft, port)
	//&pb.ClientRequest{cmd: ,timestamp: time_now,clientID: id}
	// Clear KVC
	time_now := time.Now().UnixNano()
	kvc.Clear(context.Background(), &pb.Empty{}, time_now, id)
	log.Printf("Waiting for clearing")
	res, err := acceptResult(&mappedSeq, &mappedVal, &pbft)
	if err != nil {
		log.Fatalf("Could not clear")
	}
	log.Printf("Done clearing")
	// Put setting hello -> 1
	time_now = time.Now().UnixNano()
	putReq := &pb.KeyValue{Key: "hello", Value: "1"}
	kvc.Set(context.Background(), putReq, time_now, id)
	log.Printf("Waiting for putting")
	res, err := acceptResult(&mappedSeq, &mappedVal, &pbft)
	if err != nil {
		log.Fatalf("Put error")
	}
	log.Printf("Done putting")
	log.Printf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
	if res.GetKv().Key != "hello" || res.GetKv().Value != "1" {
		log.Fatalf("Put returned the wrong response")
	}

	// Request value for hello
	time_now = time.Now().UnixNano()
	req := &pb.Key{Key: "hello"}
	kvc.Get(context.Background(), req, time_now, id)
	log.Printf("Waiting for getting")
	res, err := acceptResult(&mappedSeq, &mappedVal, &pbft)
	if err != nil {
		log.Fatalf("Request error %v", err)
	}
	log.Printf("Done getting")
	log.Printf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
	if res.GetKv().Key != "hello" || res.GetKv().Value != "1" {
		log.Fatalf("Get returned the wrong response")
	}

	// Successfully CAS changing hello -> 2
	time_now = time.Now().UnixNano()
	casReq := &pb.CASArg{Kv: &pb.KeyValue{Key: "hello", Value: "1"}, Value: &pb.Value{Value: "2"}}
	kvc.CAS(context.Background(), casReq, time_now, id)
	log.Printf("Waiting for CASing")
	res, err := acceptResult(&mappedSeq, &mappedVal, &pbft)
	if err != nil {
		log.Fatalf("Request error %v", err)
	}
	log.Printf("Done CASing")
	log.Printf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
	if res.GetKv().Key != "hello" || res.GetKv().Value != "2" {
		log.Fatalf("Get returned the wrong response")
	}

	// Unsuccessfully CAS
	time_now = time.Now().UnixNano()
	casReq = &pb.CASArg{Kv: &pb.KeyValue{Key: "hello", Value: "1"}, Value: &pb.Value{Value: "3"}}
	kvc.CAS(context.Background(), casReq, time_now, id)
	log.Printf("Waiting for CASing")
	res, err := acceptResult(&mappedSeq, &mappedVal, &pbft)
	if err != nil {
		log.Fatalf("Request error %v", err)
	}
	log.Printf("Done CASing")
	log.Printf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
	if res.GetKv().Key != "hello" || res.GetKv().Value == "3" {
		log.Fatalf("Get returned the wrong response")
	}

	// CAS should fail for uninitialized variables
	time_now = time.Now().UnixNano()
	casReq = &pb.CASArg{Kv: &pb.KeyValue{Key: "hellooo", Value: "1"}, Value: &pb.Value{Value: "2"}}
	kvc.CAS(context.Background(), casReq, time_now, id)
	log.Printf("Waiting for CASing")
	res, err := acceptResult(&mappedSeq, &mappedVal, &pbft)
	if err != nil {
		log.Fatalf("Request error %v", err)
	}
	log.Printf("Done CASing")
	log.Printf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
	if res.GetKv().Key != "hellooo" || res.GetKv().Value == "2" {
		log.Fatalf("Get returned the wrong response")
	}
}
