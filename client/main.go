package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	context "golang.org/x/net/context"
	// "google.golang.org/grpc"

	"github.com/nyu-distributed-systems-fa18/DistProject/pb"
	"github.com/nyu-distributed-systems-fa18/DistProject/util"

	// "DistProject/pb"
	// "DistProject/util"
	// "context"

	"google.golang.org/grpc"
)

type Validation struct {
	t int64
	k string
	v string
}

func usage() {
	fmt.Printf("Usage %s <endpoint>\n", os.Args[0])
	flag.PrintDefaults()
}

//res.GetKv().Key, res.GetKv().Value

func compare(v1 Validation, v2 Validation) bool {
	if v1.t == v2.t {
		if v1.k == v2.k {
			if v1.v == v2.v {
				return true
			}
		}
	}
	return false
}

func acceptResult(mapS map[int64]int64, mapV map[int64]Validation, r *util.Pbft) (*pb.Result, error) {
	numberOfValidResponses := int64(0)
	val := pb.Result{}
	for numberOfValidResponses < 2 { //lot of changes required for a better performance
		log.Printf("waiting for a response")
		select {
		case res := <-r.ResponseChan:
			log.Printf("got a response")
			if v := mapS[res.SequenceID]; v < 2 {
				check := Validation{t: res.Timestamp, k: res.NodeResult.GetKv().Key, v: res.NodeResult.GetKv().Value}
				if v > 0 {
					checkee := mapV[v-1]
					if compare(checkee, check) {
						mapS[res.SequenceID] = v + 1
						numberOfValidResponses = v + 1
						mapV[res.SequenceID] = check
						val = *res.NodeResult
					}
				} else {
					mapS[res.SequenceID] = v + 1
					numberOfValidResponses = v + 1
					mapV[res.SequenceID] = check
					val = *res.NodeResult
				}

			}
			// default:
			// 	continue
		}
	}
	return &val, nil
}

func main() {
	// Take endpoint as input
	mappedSeq := make(map[int64]int64)
	mappedVal := make(map[int64]Validation)
	var primary string
	var pbftPort int
	flag.IntVar(&pbftPort, "pbft", 3008,
		"Port on which server should listen to Pbft requests")
	flag.StringVar(&primary, "primary", "127.0.0.1:3000",
		"Pbft Primary call")
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lshortfile)

	name, err := os.Hostname()
	if err != nil {
		log.Fatalf("Could not get hostname")
	}
	id := fmt.Sprintf("%s:%d", name, pbftPort)
	log.Printf("Starting the client with ID %s", id)
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
	pbft := util.Pbft{PrePrepareMsgChan: make(chan util.PrePrepareMsgInput), PrepareMsgChan: make(chan util.PrepareMsgInput), CommitMsgChan: make(chan util.CommitMsgInput), ViewChangeMsgChan: make(chan util.ViewChangeMsgInput), ResponseChan: make(chan *pb.ClientResponse)}
	go util.RunPbftServer(&pbft, port)
	//&pb.ClientRequest{cmd: ,timestamp: time_now,clientID: id}
	// Clear KVC

	//CLEAR
	time_now := time.Now().UnixNano()
	in := &pb.Empty{}
	r := pb.Command{Operation: pb.Op_CLEAR, Arg: &pb.Command_Clear{Clear: in}}
	c := pb.ClientRequest{Cmd: &r, Timestamp: time_now, ClientID: id}
	kvc.Call(context.Background(), &c)
	log.Printf("Waiting for clearing")
	res, err := acceptResult(mappedSeq, mappedVal, &pbft)
	if err != nil {
		log.Fatalf("Could not clear")
	}
	log.Printf("Done clearing")

	// Put setting hello -> 1
	time_now = time.Now().UnixNano()
	in2 := &pb.KeyValue{Key: "hello", Value: "1"}
	r = pb.Command{Operation: pb.Op_SET, Arg: &pb.Command_Set{Set: in2}}
	c = pb.ClientRequest{Cmd: &r, Timestamp: time_now, ClientID: id}
	kvc.Call(context.Background(), &c)
	log.Printf("Waiting for putting")
	res, err = acceptResult(mappedSeq, mappedVal, &pbft)
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
	in3 := &pb.Key{Key: "hello"}
	r = pb.Command{Operation: pb.Op_GET, Arg: &pb.Command_Get{Get: in3}}
	c = pb.ClientRequest{Cmd: &r, Timestamp: time_now, ClientID: id}
	kvc.Call(context.Background(), &c)
	log.Printf("Waiting for getting")
	res, err = acceptResult(mappedSeq, mappedVal, &pbft)
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
	in4 := &pb.CASArg{Kv: &pb.KeyValue{Key: "hello", Value: "1"}, Value: &pb.Value{Value: "2"}}
	r = pb.Command{Operation: pb.Op_CAS, Arg: &pb.Command_Cas{Cas: in4}}
	c = pb.ClientRequest{Cmd: &r, Timestamp: time_now, ClientID: id}
	kvc.Call(context.Background(), &c)
	log.Printf("Waiting for CASing")
	res, err = acceptResult(mappedSeq, mappedVal, &pbft)
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
	in5 := &pb.CASArg{Kv: &pb.KeyValue{Key: "hello", Value: "1"}, Value: &pb.Value{Value: "3"}}
	r = pb.Command{Operation: pb.Op_CAS, Arg: &pb.Command_Cas{Cas: in5}}
	c = pb.ClientRequest{Cmd: &r, Timestamp: time_now, ClientID: id}
	kvc.Call(context.Background(), &c)
	log.Printf("Waiting for CASing")
	res, err = acceptResult(mappedSeq, mappedVal, &pbft)
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
	in6 := &pb.CASArg{Kv: &pb.KeyValue{Key: "hellooo", Value: "1"}, Value: &pb.Value{Value: "2"}}
	r = pb.Command{Operation: pb.Op_CAS, Arg: &pb.Command_Cas{Cas: in6}}
	c = pb.ClientRequest{Cmd: &r, Timestamp: time_now, ClientID: id}
	kvc.Call(context.Background(), &c)
	log.Printf("Waiting for CASing")
	res, err = acceptResult(mappedSeq, mappedVal, &pbft)
	if err != nil {
		log.Fatalf("Request error %v", err)
	}
	log.Printf("Done CASing")
	log.Printf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
	if res.GetKv().Key != "hellooo" || res.GetKv().Value == "2" {
		log.Fatalf("Get returned the wrong response")
	}
}
