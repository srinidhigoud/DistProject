package main

import (
	"fmt"
	"log"
	rand "math/rand"
	"net"
	"time"
	context "golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/nyu-distributed-systems-fa18/lab-2-raft-srinidhigoud/pb"
)




type Pbft struct {
	ClientRequestChan chan ClientRequestInput
	PrePrepareMsgChan chan PrePrepareMsgInput
	PrepareMsgChan    chan PrepareMsgInput
	CommitMsgChan     chan CommitMsgInput
}

func (r *Pbft) ClientRequestPBFT(ctx context.Context, arg *pb.ClientRequest) (*pb.ClientResponse, error) {
	log.Printf("Inside ClientRequestPBFT arg %v ", arg)
	c := make(chan pb.ClientResponse)
	r.ClientRequestChan <- ClientRequestInput{Arg: arg, Response: c}
	result := <-c
	return &result, nil
}

func (r *Pbft) PrePreparePBFT(ctx context.Context, arg *pb.PrePrepareMsg) (*pb.PbftMsgAccepted, error) {
	c := make(chan pb.PbftMsgAccepted)
	r.PrePrepareMsgChan <- PrePrepareMsgInput{Arg: arg, Response: c}
	result := <-c
	return &result, nil
}

func (r *Pbft) PreparePBFT(ctx context.Context, arg *pb.PrepareMsg) (*pb.PbftMsgAccepted, error) {
	c := make(chan pb.PbftMsgAccepted)
	r.PrepareMsgChan <- PrepareMsgInput{Arg: arg, Response: c}
	result := <-c
	return &result, nil
}

func (r *Pbft) CommitPBFT(ctx context.Context, arg *pb.CommitMsg) (*pb.PbftMsgAccepted, error) {
	c := make(chan pb.PbftMsgAccepted)
	r.CommitMsgChan <- CommitMsgInput{Arg: arg, Response: c}
	result := <-c
	return &result, nil
}

func RandomDuration(r *rand.Rand) time.Duration {
	const DurationMax = 40000
	const DurationMin = 10000
	return time.Duration(r.Intn(DurationMax-DurationMin)+DurationMin) * time.Millisecond
}

func RestartTimer(timer *time.Timer, r *rand.Rand) {
	stopped := timer.Stop()
	if !stopped {
		for len(timer.C) > 0 {
			<-timer.C
		}
	}
	timer.Reset(RandomDuration(r))
}


func RunPbftServer(r *Pbft, port int) {
	portString := fmt.Sprintf(":%d", port)
	c, err := net.Listen("tcp", portString)
	if err != nil {
		log.Fatalf("Could not create listening socket %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterPbftServer(s, r)
	log.Printf("Going to listen on port %v", port)
	if err := s.Serve(c); err != nil {
		log.Fatalf("Failed to serve %v", err)
	}
}

func ConnectToPeer(peer string) (pb.PbftClient, error) {
	backoffConfig := grpc.DefaultBackoffConfig
	backoffConfig.MaxDelay = 500 * time.Millisecond
	conn, err := grpc.Dial(peer, grpc.WithInsecure(), grpc.WithBackoffConfig(backoffConfig))
	log.Printf("ConnectToPeer peer %v", peer)
	log.Printf("ConnectToPeer err %v", err)
	log.Printf("ConnectToPeer conn %v", conn)
	if err != nil {
		log.Printf("ConnectToPeer err %v", err)
		return pb.NewPbftClient(nil), err
	}
	return pb.NewPbftClient(conn), nil
}


func serve(s *KVStore, r *rand.Rand, peers *ArrayPeers, id string, port int, client string, kvs *KvStoreServer) {
	pbft := Pbft{ClientRequestChan: make(chan ClientRequestInput), PrePrepareMsgChan: make(chan PrePrepareMsgInput), PrepareMsgChan: make(chan PrepareMsgInput), CommitMsgChan: make(chan CommitMsgInput)}
	go RunPbftServer(&pbft, port)
	peerClients := make(map[string]pb.PbftClient)
	for _, peer := range *peers {
		clientPeer, err := ConnectToPeer(peer)
		if err != nil {
			log.Fatalf("Failed to connect to GRPC server %v", err)
		}
		peerClients[peer] = clientPeer
		log.Printf("Connected to %v", peer)
	}

	clientConn, _ := ConnectToPeer(client)
	peerClients[client] = clientConn

	// Is this needed??
	// type ClientResponse struct {
	// 	ret  *pb.ClientResponse
	// 	err  error
	// 	peer string
	// }

	type PbftMsgAccepted struct {
		ret  *pb.PbftMsgAccepted
		err  error
		peer string
	}
	// clientResponseChan := make(chan ClientResponse)
	pbftMsgAcceptedChan := make(chan PbftMsgAccepted)

	// Create a timer and start running it
	timer := time.NewTimer(RandomDuration(r))

	for {
		select {
		case <-timer.C:
			// log.Printf("Timeout")
			// for p, c := range peerClients {
			// 	// go func(c pb.PbftClient, p string) {
			// 	// 	ret, err := c.RequestVote(context.Background(), &pb.RequestVoteArgs{Term: 1, CandidateID: id})
			// 	// 	voteResponseChan <- VoteResponse{ret: ret, err: err, peer: p}
			// 	// }(c, p)
			// }
			RestartTimer(timer, r)
		case op := <-s.C:
			s.HandleCommand(op)
		// Should this be only in client?? pbftClr := <-pbft.ClientRequestChan
		case pbftClr := <-pbft.ClientRequestChan:
			log.Printf("Received ClientRequestChan %v", pbftClr.Arg.ClientID)
			// Do something here as primary to return initially to client
			clientId := pbftClr.Arg.ClientID
			op := strings.Split(pbftClr.Arg.Operation, ":")
			timestamp := pbftClr.Arg.Timestamp
			operation := op[0]
			key := op[1]
			val := op[2]
			res := pb.Result{Result: &pb.Result_S{S: &pb.Success{}}}
			if operation == "set" {
				kvs.Store[key] = val
				// reset res
				res = pb.Result{Result: &pb.Result_Kv{Kv: &pb.KeyValue{Key: key, Value: val}}}
			} else if operation == "get" {
				// reset res
				val = kvs.Store[key]
				res = pb.Result{Result: &pb.Result_Kv{Kv: &pb.KeyValue{Key: key, Value: val}}}
			}
			log.Printf("Received pbft client (%v) req for %v at %v", clientId, operation, timestamp)

			responseBack := pb.ClientResponse{ViewId: 1, Timestamp: timestamp, ClientID: clientId, Node: id, NodeResult: &res}
			log.Printf("Sending back responseBack - %v", responseBack)
			pbftClr.Response <- responseBack
		case pbftPrePrep := <-pbft.PrePrepareMsgChan:
			log.Printf("Received PrePrepareMsgChan %v", pbftPrePrep.Arg.Node)
		case pbftPre := <-pbft.PrepareMsgChan:
			log.Printf("Received PrePrepareMsgChan %v", pbftPre.Arg.Node)
		case pbftCom := <-pbft.CommitMsgChan:
			log.Printf("Received PrePrepareMsgChan %v", pbftCom.Arg.Node)
		// Is this needed??
		// case clr := <-clientResponseChan:
		// 	log.Printf("Client Request Received")
		// 	// log.Printf("Client Request Received %v", clr.peer)
		case pbftMsg := <-pbftMsgAcceptedChan:
			// log.Printf("Some PBFT Msg Acceptance Received")
			log.Printf("Some PBFT Msg Acceptance Received %v, %v", pbftMsg.ret.TypeOfAccepted, pbftMsg.ret.Node)
		}
	}
	log.Printf("Strange to arrive here")
}