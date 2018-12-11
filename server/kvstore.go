package main

import (
	"log"

	context "golang.org/x/net/context"
	// "context"
	"github.com/nyu-distributed-systems-fa18/DistProject/pb"
	"github.com/nyu-distributed-systems-fa18/DistProject/util"
	// "DistProject/pb"
	// "DistProject/util"
)

// The struct for data to send over channel
type InputChannelType struct {
	clientRequest *pb.ClientRequest
}

// The struct for key value stores.
type KVStore struct {
	C     chan InputChannelType
	store map[string]string
}

func (s *KVStore) Call(ctx context.Context, c *pb.ClientRequest) (*pb.Success, error) {
	// Send request over the channel
	log.Printf("Client just called")
	s.C <- InputChannelType{clientRequest: c}
	result := pb.Success{}
	return &result, nil
}

// func (s *KVStore) Get(ctx context.Context, key *pb.Key, time int64, id string) {
// 	// Create a request
// 	r := pb.Command{Operation: pb.Op_GET, Arg: &pb.Command_Get{Get: key}}
// 	c := pb.ClientRequest(cmd: r, timestamp: time, clientID: id)
// 	// Send request over the channel
// 	s.C <- InputChannelType{clientRequest: c}
// }

// func (s *KVStore) Set(ctx context.Context, in *pb.KeyValue, time int64, id string)) {
// 	// Create a request
// 	r := pb.Command{Operation: pb.Op_SET, Arg: &pb.Command_Set{Set: in}}
// 	c := pb.ClientRequest(cmd: r, timestamp: time, clientID: id)
// 	// Send request over the channel
// 	s.C <- InputChannelType{clientRequest: c}
// }

// func (s *KVStore) Clear(ctx context.Context, in *pb.Empty, time int64, id string)) {
// 	// Create a request
// 	r := pb.Command{Operation: pb.Op_CLEAR, Arg: &pb.Command_Clear{Clear: in}}
// 	c := pb.ClientRequest(cmd: r, timestamp: time, clientID: id)
// 	// Send request over the channel
// 	s.C <- InputChannelType{clientRequest: c}
// }

// func (s *KVStore) CAS(ctx context.Context, in *pb.CASArg, time int64, id string)){
// 	// Create a request
// 	r := pb.Command{Operation: pb.Op_CAS, Arg: &pb.Command_Cas{Cas: in}}
// 	c := pb.ClientRequest(cmd: r, timestamp: time, clientID: id)
// 	// Send request over the channel
// 	s.C <- InputChannelType{clientRequest: c}
// }

// Used internally to generate a result for a get request. This function assumes that it is called from a single thread of
// execution, and hence does not handle races.
func (s *KVStore) GetInternal(k string) pb.Result {
	v := s.store[k]
	return pb.Result{Result: &pb.Result_Kv{Kv: &pb.KeyValue{Key: k, Value: v}}}
}

// Used internally to set and generate an appropriate result. This function assumes that it is called from a single
// thread of execution and hence does not handle race conditions.
func (s *KVStore) SetInternal(k string, v string) pb.Result {
	s.store[k] = v
	return pb.Result{Result: &pb.Result_Kv{Kv: &pb.KeyValue{Key: k, Value: v}}}
}

// Used internally, this function clears a kv store. Assumes no racing calls.
func (s *KVStore) ClearInternal() pb.Result {
	s.store = make(map[string]string)
	return pb.Result{Result: &pb.Result_S{S: &pb.Success{}}}
}

// Used internally this function performs CAS assuming no races.
func (s *KVStore) CasInternal(k string, v string, vn string) pb.Result {
	vc := s.store[k]
	if vc == v {
		s.store[k] = vn
		return pb.Result{Result: &pb.Result_Kv{Kv: &pb.KeyValue{Key: k, Value: vn}}}
	} else {
		return pb.Result{Result: &pb.Result_Kv{Kv: &pb.KeyValue{Key: k, Value: vc}}}
	}
}

func (s *KVStore) HandleCommand(req *pb.ClientRequest, viewId int64, node string, sequenceID int64) {
	// req := ip.clientRequest
	timestamp := req.Timestamp
	clientID := req.ClientID
	// op := req.cmd
	switch c := req.Cmd; c.Operation {
	case pb.Op_GET:
		arg := c.GetGet()
		result := s.GetInternal(arg.Key)
		client, err := util.ConnectToClient(clientID) //client connection
		if err != nil {
			log.Fatalf("Failed to connect to GRPC server %v", err)
		}
		log.Printf("Connected to %v", clientID)
		go func(c pb.PbftClient) {
			c.SendResponseBack(context.Background(),
				&pb.ClientResponse{ViewId: viewId, Timestamp: timestamp, ClientID: clientID, Node: node, NodeResult: &result, SequenceID: sequenceID})
		}(client)
	case pb.Op_SET:
		arg := c.GetSet()
		result := s.SetInternal(arg.Key, arg.Value)
		client, err := util.ConnectToClient(clientID) //client connection
		if err != nil {
			log.Fatalf("Failed to connect to GRPC server %v", err)
		}
		log.Printf("Connected to %v", clientID)
		go func(c pb.PbftClient) {
			c.SendResponseBack(context.Background(),
				&pb.ClientResponse{ViewId: viewId, Timestamp: timestamp, ClientID: clientID, Node: node, NodeResult: &result, SequenceID: sequenceID})
		}(client)
	case pb.Op_CLEAR:
		result := s.ClearInternal()
		client, err := util.ConnectToClient(clientID) //client connection
		if err != nil {
			log.Fatalf("Failed to connect to GRPC server %v", err)
		}
		log.Printf("Connected to %v", clientID)
		go func(c pb.PbftClient) {
			c.SendResponseBack(context.Background(),
				&pb.ClientResponse{ViewId: viewId, Timestamp: timestamp, ClientID: clientID, Node: node, NodeResult: &result, SequenceID: sequenceID})
		}(client)
	case pb.Op_CAS:
		arg := c.GetCas()
		result := s.CasInternal(arg.Kv.Key, arg.Kv.Value, arg.Value.Value)
		client, err := util.ConnectToClient(clientID) //client connection
		if err != nil {
			log.Fatalf("Failed to connect to GRPC server %v", err)
		}
		log.Printf("Connected to %v", clientID)
		go func(c pb.PbftClient) {
			c.SendResponseBack(context.Background(),
				&pb.ClientResponse{ViewId: viewId, Timestamp: timestamp, ClientID: clientID, Node: node, NodeResult: &result, SequenceID: sequenceID})
		}(client)
	default:
		// Sending a blank response to just free things up, but we don't know how to make progress here.
		result := pb.Result{}
		client, err := util.ConnectToClient(clientID) //client connection
		if err != nil {
			log.Fatalf("Failed to connect to GRPC server %v", err)
		}
		log.Printf("Connected to %v", clientID)
		go func(c pb.PbftClient) {
			c.SendResponseBack(context.Background(),
				&pb.ClientResponse{ViewId: viewId, Timestamp: timestamp, ClientID: clientID, Node: node, NodeResult: &result, SequenceID: sequenceID})
		}(client)
		log.Fatalf("Unrecognized operation %v", c)
	}
}
