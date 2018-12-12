package main

import (
	"flag"
	"fmt"
	"log"
	rand "math/rand"
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
	t   int64
	res pb.Result
}

func usage() {
	fmt.Printf("Usage %s <endpoint>\n", os.Args[0])
	flag.PrintDefaults()
}

//res.GetKv().Key, res.GetKv().Value

func compare(v1 Validation, v2 Validation) bool {
	return true

	// if v1.t == v2.t {
	// 	if v1.k == v2.k {
	// 		if v1.v == v2.v {
	// 			return true
	// 		}
	// 	}
	// }
	// return false
}

func acceptResult(mapS map[int64]int64, mapV map[int64]Validation, r *util.Pbft) (*pb.Result, error, string) {
	var rd *rand.Rand
	rd = rand.New(rand.NewSource(time.Now().UnixNano()))
	numberOfValidResponses := int64(0)
	val := pb.Result{}
	clientTimer := util.NewSecondsTimer(util.RandomDuration(rd))
	// clientTimer.Stop()
	for numberOfValidResponses < 2 { //lot of changes required for a better performance
		log.Printf("waiting for a response")
		select {
		case <-clientTimer.Timer.C:
			primary := "Broadcast"
			log.Printf("Broadcasting now to all", primary)
			clientTimer.Stop()
			return &val, nil, primary
		case inputChan := <-r.PbftMsgChan:
			if clientTimer.TimeRemaining() < 10*time.Millisecond {
				dur := util.RandomDuration(rd)
				log.Printf("Resetting timer for duration - %v", dur)
				clientTimer.Reset(dur)
			}
			msg := inputChan.Arg
			op := msg.Operation
			res := msg.GetCrm()
			log.Printf("got a response")
			if res.SequenceID < 0 || op == "Redirect" {

				primary := res.NodeResult.GetRedirect().GetServer()
				log.Printf("redirect now to %v", primary)
				clientTimer.Stop()
				return &val, nil, primary
			} else if v := mapS[res.SequenceID]; v < 2 {
				log.Printf("got in") //res.NodeResult.GetKv().Key
				check := Validation{t: res.Timestamp, res: *res.NodeResult}
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
	clientTimer.Stop()
	return &val, nil, ""
}

func main() {
	// Take endpoint as input
	mappedSeq := make(map[int64]int64)
	mappedVal := make(map[int64]Validation)
	var primary string
	var pbftPort int
	flag.IntVar(&pbftPort, "pbft", 3000,
		"Port on which server should listen to Pbft requests")
	flag.StringVar(&primary, "primary", "127.0.0.1:3005",
		"Pbft Primary call")
	flag.Parse()

	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// name, err := os.Hostname()
	// if err != nil {
	// 	log.Fatalf("Could not get hostname")
	// }
	id := fmt.Sprintf("%d", pbftPort)
	id = "127.0.0.1:" + id
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

	// port := 3000
	res := &pb.Result{}
	newprimary := ""
	pbft := util.Pbft{PbftMsgChan: make(chan util.PbftMsgInput)}
	go util.RunPbftServer(&pbft, pbftPort)
	//&pb.ClientRequest{cmd: ,timestamp: time_now,clientID: id}
	// Clear KVC

	//CLEAR
	time_now := time.Now().UnixNano()
	in := &pb.Empty{}
	r := pb.Command{Operation: pb.Op_CLEAR, Arg: &pb.Command_Clear{Clear: in}}
	c := pb.ClientRequest{Cmd: &r, Timestamp: time_now, ClientID: id}
	kvc.Call(context.Background(), &c)
	log.Printf("Waiting for clearing")
	redirected := false
	for {
		res, err, newprimary = acceptResult(mappedSeq, mappedVal, &pbft)
		if err != nil {
			log.Fatalf("Could not clear")
			break
		} else if newprimary == "" {
			break
		} else if newprimary == "Broadcast" {
			conn, err = grpc.Dial("127.0.0.1:3005", grpc.WithInsecure())
			if err != nil {
				log.Fatalf("Failed to dial GRPC server %v", err)
			}
			log.Printf("Connected")
			kvc = pb.NewKvStoreClient(conn)
			kvc.Call(context.Background(), &c)
			log.Printf("Waiting for clearing")
			conn, err = grpc.Dial("127.0.0.1:3006", grpc.WithInsecure())
			if err != nil {
				log.Fatalf("Failed to dial GRPC server %v", err)
			}
			log.Printf("Connected")
			kvc = pb.NewKvStoreClient(conn)
			kvc.Call(context.Background(), &c)
			log.Printf("Waiting for clearing")
			conn, err = grpc.Dial("127.0.0.1:3007", grpc.WithInsecure())
			if err != nil {
				log.Fatalf("Failed to dial GRPC server %v", err)
			}
			log.Printf("Connected")
			kvc = pb.NewKvStoreClient(conn)
			kvc.Call(context.Background(), &c)
			log.Printf("Waiting for clearing")
			conn, err = grpc.Dial("127.0.0.1:3008", grpc.WithInsecure())
			if err != nil {
				log.Fatalf("Failed to dial GRPC server %v", err)
			}
			log.Printf("Connected")
			kvc = pb.NewKvStoreClient(conn)
			kvc.Call(context.Background(), &c)
			log.Printf("Waiting for clearing")
		} else if !redirected {
			redirected = true
			conn, err = grpc.Dial("127.0.0.1:"+newprimary, grpc.WithInsecure())
			if err != nil {
				log.Fatalf("Failed to dial GRPC server %v", err)
			}
			log.Printf("Connected")
			kvc = pb.NewKvStoreClient(conn)
			kvc.Call(context.Background(), &c)
			log.Printf("Waiting for clearing")
		} else {
			break
		}
	}

	log.Printf("Done clearing")

	// Put setting hello -> 1
	time_now = time.Now().UnixNano()
	in2 := &pb.KeyValue{Key: "hello", Value: "1"}
	r = pb.Command{Operation: pb.Op_SET, Arg: &pb.Command_Set{Set: in2}}
	c = pb.ClientRequest{Cmd: &r, Timestamp: time_now, ClientID: id}
	kvc.Call(context.Background(), &c)
	log.Printf("Waiting for putting")
	redirected = false
	for {
		res, err, newprimary = acceptResult(mappedSeq, mappedVal, &pbft)
		if err != nil {
			log.Fatalf("Put error")
			break
		} else if newprimary == "" {
			break
		} else if newprimary == "Broadcast" {
			conn, err = grpc.Dial("127.0.0.1:3005", grpc.WithInsecure())
			if err != nil {
				log.Fatalf("Failed to dial GRPC server %v", err)
			}
			log.Printf("Connected")
			kvc = pb.NewKvStoreClient(conn)
			kvc.Call(context.Background(), &c)
			log.Printf("Waiting for putting")
			conn, err = grpc.Dial("127.0.0.1:3006", grpc.WithInsecure())
			if err != nil {
				log.Fatalf("Failed to dial GRPC server %v", err)
			}
			log.Printf("Connected")
			kvc = pb.NewKvStoreClient(conn)
			kvc.Call(context.Background(), &c)
			log.Printf("Waiting for putting")
			conn, err = grpc.Dial("127.0.0.1:3007", grpc.WithInsecure())
			if err != nil {
				log.Fatalf("Failed to dial GRPC server %v", err)
			}
			log.Printf("Connected")
			kvc = pb.NewKvStoreClient(conn)
			kvc.Call(context.Background(), &c)
			log.Printf("Waiting for putting")
			conn, err = grpc.Dial("127.0.0.1:3008", grpc.WithInsecure())
			if err != nil {
				log.Fatalf("Failed to dial GRPC server %v", err)
			}
			log.Printf("Connected")
			kvc = pb.NewKvStoreClient(conn)
			kvc.Call(context.Background(), &c)
			log.Printf("Waiting for putting")
		} else if !redirected {
			redirected = true
			conn, err = grpc.Dial("127.0.0.1:"+newprimary, grpc.WithInsecure())
			if err != nil {
				log.Fatalf("Failed to dial GRPC server %v", err)
			}
			log.Printf("Connected")
			kvc = pb.NewKvStoreClient(conn)
			kvc.Call(context.Background(), &c)
			log.Printf("Waiting for putting")
		} else {
			break
		}
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
	redirected = false
	for {
		res, err, newprimary = acceptResult(mappedSeq, mappedVal, &pbft)
		if err != nil {
			log.Fatalf("Request error %v", err)
			break
		} else if newprimary == "" {
			break
		} else if newprimary == "Broadcast" {
			conn, err = grpc.Dial("127.0.0.1:3005", grpc.WithInsecure())
			if err != nil {
				log.Fatalf("Failed to dial GRPC server %v", err)
			}
			log.Printf("Connected")
			kvc = pb.NewKvStoreClient(conn)
			kvc.Call(context.Background(), &c)
			log.Printf("Waiting for getting")
			conn, err = grpc.Dial("127.0.0.1:3006", grpc.WithInsecure())
			if err != nil {
				log.Fatalf("Failed to dial GRPC server %v", err)
			}
			log.Printf("Connected")
			kvc = pb.NewKvStoreClient(conn)
			kvc.Call(context.Background(), &c)
			log.Printf("Waiting for getting")
			conn, err = grpc.Dial("127.0.0.1:3007", grpc.WithInsecure())
			if err != nil {
				log.Fatalf("Failed to dial GRPC server %v", err)
			}
			log.Printf("Connected")
			kvc = pb.NewKvStoreClient(conn)
			kvc.Call(context.Background(), &c)
			log.Printf("Waiting for getting")
			conn, err = grpc.Dial("127.0.0.1:3008", grpc.WithInsecure())
			if err != nil {
				log.Fatalf("Failed to dial GRPC server %v", err)
			}
			log.Printf("Connected")
			kvc = pb.NewKvStoreClient(conn)
			kvc.Call(context.Background(), &c)
			log.Printf("Waiting for getting")
		} else if !redirected {
			redirected = true
			conn, err = grpc.Dial("127.0.0.1:"+newprimary, grpc.WithInsecure())
			if err != nil {
				log.Fatalf("Failed to dial GRPC server %v", err)
			}
			log.Printf("Connected")
			kvc = pb.NewKvStoreClient(conn)
			kvc.Call(context.Background(), &c)
			log.Printf("Waiting for getting")
		} else {
			break
		}
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
	redirected = false
	for {
		res, err, newprimary = acceptResult(mappedSeq, mappedVal, &pbft)
		if err != nil {
			log.Fatalf("Request error %v", err)
			break
		} else if newprimary == "" {
			break
		} else if newprimary == "Broadcast" {
			conn, err = grpc.Dial("127.0.0.1:3005", grpc.WithInsecure())
			if err != nil {
				log.Fatalf("Failed to dial GRPC server %v", err)
			}
			log.Printf("Connected")
			kvc = pb.NewKvStoreClient(conn)
			kvc.Call(context.Background(), &c)
			log.Printf("Waiting for casing")
			conn, err = grpc.Dial("127.0.0.1:3006", grpc.WithInsecure())
			if err != nil {
				log.Fatalf("Failed to dial GRPC server %v", err)
			}
			log.Printf("Connected")
			kvc = pb.NewKvStoreClient(conn)
			kvc.Call(context.Background(), &c)
			log.Printf("Waiting for casing")
			conn, err = grpc.Dial("127.0.0.1:3007", grpc.WithInsecure())
			if err != nil {
				log.Fatalf("Failed to dial GRPC server %v", err)
			}
			log.Printf("Connected")
			kvc = pb.NewKvStoreClient(conn)
			kvc.Call(context.Background(), &c)
			log.Printf("Waiting for casing")
			conn, err = grpc.Dial("127.0.0.1:3008", grpc.WithInsecure())
			if err != nil {
				log.Fatalf("Failed to dial GRPC server %v", err)
			}
			log.Printf("Connected")
			kvc = pb.NewKvStoreClient(conn)
			kvc.Call(context.Background(), &c)
			log.Printf("Waiting for casing")
		} else if !redirected {
			redirected = true
			conn, err = grpc.Dial("127.0.0.1:"+newprimary, grpc.WithInsecure())
			if err != nil {
				log.Fatalf("Failed to dial GRPC server %v", err)
			}
			log.Printf("Connected")
			kvc = pb.NewKvStoreClient(conn)
			kvc.Call(context.Background(), &c)
			log.Printf("Waiting for casing")
		} else {
			break
		}
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
	redirected = false
	for {
		res, err, newprimary = acceptResult(mappedSeq, mappedVal, &pbft)
		if err != nil {
			log.Fatalf("Request error %v", err)
			break
		} else if newprimary == "" {
			break
		} else if newprimary == "Broadcast" {
			conn, err = grpc.Dial("127.0.0.1:3005", grpc.WithInsecure())
			if err != nil {
				log.Fatalf("Failed to dial GRPC server %v", err)
			}
			log.Printf("Connected")
			kvc = pb.NewKvStoreClient(conn)
			kvc.Call(context.Background(), &c)
			log.Printf("Waiting for casing")
			conn, err = grpc.Dial("127.0.0.1:3006", grpc.WithInsecure())
			if err != nil {
				log.Fatalf("Failed to dial GRPC server %v", err)
			}
			log.Printf("Connected")
			kvc = pb.NewKvStoreClient(conn)
			kvc.Call(context.Background(), &c)
			log.Printf("Waiting for casing")
			conn, err = grpc.Dial("127.0.0.1:3007", grpc.WithInsecure())
			if err != nil {
				log.Fatalf("Failed to dial GRPC server %v", err)
			}
			log.Printf("Connected")
			kvc = pb.NewKvStoreClient(conn)
			kvc.Call(context.Background(), &c)
			log.Printf("Waiting for casing")
			conn, err = grpc.Dial("127.0.0.1:3008", grpc.WithInsecure())
			if err != nil {
				log.Fatalf("Failed to dial GRPC server %v", err)
			}
			log.Printf("Connected")
			kvc = pb.NewKvStoreClient(conn)
			kvc.Call(context.Background(), &c)
			log.Printf("Waiting for casing")
		} else if !redirected {
			redirected = true
			conn, err = grpc.Dial("127.0.0.1:"+newprimary, grpc.WithInsecure())
			if err != nil {
				log.Fatalf("Failed to dial GRPC server %v", err)
			}
			log.Printf("Connected")
			kvc = pb.NewKvStoreClient(conn)
			kvc.Call(context.Background(), &c)
			log.Printf("Waiting for casing")
		} else {
			break
		}
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
	redirected = false
	for {
		res, err, newprimary = acceptResult(mappedSeq, mappedVal, &pbft)
		if err != nil {
			log.Fatalf("Request error %v", err)
			break
		} else if newprimary == "" {
			break
		} else if newprimary == "Broadcast" {
			conn, err = grpc.Dial("127.0.0.1:3005", grpc.WithInsecure())
			if err != nil {
				log.Fatalf("Failed to dial GRPC server %v", err)
			}
			log.Printf("Connected")
			kvc = pb.NewKvStoreClient(conn)
			kvc.Call(context.Background(), &c)
			log.Printf("Waiting for casing")
			conn, err = grpc.Dial("127.0.0.1:3006", grpc.WithInsecure())
			if err != nil {
				log.Fatalf("Failed to dial GRPC server %v", err)
			}
			log.Printf("Connected")
			kvc = pb.NewKvStoreClient(conn)
			kvc.Call(context.Background(), &c)
			log.Printf("Waiting for casing")
			conn, err = grpc.Dial("127.0.0.1:3007", grpc.WithInsecure())
			if err != nil {
				log.Fatalf("Failed to dial GRPC server %v", err)
			}
			log.Printf("Connected")
			kvc = pb.NewKvStoreClient(conn)
			kvc.Call(context.Background(), &c)
			log.Printf("Waiting for casing")
			conn, err = grpc.Dial("127.0.0.1:3008", grpc.WithInsecure())
			if err != nil {
				log.Fatalf("Failed to dial GRPC server %v", err)
			}
			log.Printf("Connected")
			kvc = pb.NewKvStoreClient(conn)
			kvc.Call(context.Background(), &c)
			log.Printf("Waiting for casing")
		} else if !redirected {
			redirected = true
			conn, err = grpc.Dial("127.0.0.1:"+newprimary, grpc.WithInsecure())
			if err != nil {
				log.Fatalf("Failed to dial GRPC server %v", err)
			}
			log.Printf("Connected")
			kvc = pb.NewKvStoreClient(conn)
			kvc.Call(context.Background(), &c)
			log.Printf("Waiting for casing")
		} else {
			break
		}
	}
	log.Printf("Done CASing")
	log.Printf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
	if res.GetKv().Key != "hellooo" || res.GetKv().Value == "2" {
		log.Fatalf("Get returned the wrong response")
	}
}
