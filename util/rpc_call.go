package util

import (
	"fmt"
	"log"
	rand "math/rand"
	"net"
	"time"
	context "golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/nyu-distributed-systems-fa18/DistProject/pb"
)

// Messages that can be passed from the PbftLocal RPC server to the main loop for AppendEntries
type AppendEntriesInput struct {
	Arg      *pb.AppendEntriesArgs
	Response chan pb.AppendEntriesRet
}

// Messages that can be passed from the PbftLocal RPC server to the main loop for VoteInput
type VoteInput struct {
	Arg      *pb.RequestVoteArgs
	Response chan pb.RequestVoteRet
}

// Struct off of which we shall hang the PbftLocal service
type PbftLocal struct {
	AppendChan chan AppendEntriesInput
	VoteChan   chan VoteInput
}

type PbftGlobal struct {
	ResponseChan chan pb.ClientResponse
}

func (r *PbftLocal) AppendEntries(ctx context.Context, Arg *pb.AppendEntriesArgs) (*pb.AppendEntriesRet, error) {
	c := make(chan pb.AppendEntriesRet)
	r.AppendChan <- AppendEntriesInput{Arg: Arg, Response: c}
	result := <-c
	return &result, nil
}

func (r *PbftLocal) RequestVote(ctx context.Context, Arg *pb.RequestVoteArgs) (*pb.RequestVoteRet, error) {
	c := make(chan pb.RequestVoteRet)
	r.VoteChan <- VoteInput{Arg: Arg, Response: c}
	result := <-c
	return &result, nil
}

func (r *PbftGlobal) SendResponseBack(ctx context.Context, Arg *pb.ClientResponse) {
	r.AppendChan <- Arg
}



// Compute a random duration in milliseconds
func RandomDuration(r *rand.Rand, heartbeat bool) time.Duration {
	// Constant
	if heartbeat{
		const DurationMax = 5000
		const DurationMin = 1000
		return time.Duration(r.Intn(DurationMax-DurationMin)+DurationMin) * time.Millisecond
	} else {
		const DurationMax = 50000
		const DurationMin = 10000
		return time.Duration(r.Intn(DurationMax-DurationMin)+DurationMin) * time.Millisecond
	}
	
}


// Restart the supplied timer using a random timeout based on function above
func RestartTimer(timer *time.Timer, r *rand.Rand, heartbeat bool) {
	stopped := timer.Stop()
	// If stopped is false that means someone stopped before us, which could be due to the timer going off before this,
	// in which case we just drain notifications.
	if !stopped {
		// Loop for any queued notifications
		for len(timer.C) > 0 {
			<-timer.C
		}

	}
	timer.Reset(RandomDuration(r, heartbeat))
}



// Launch a GRPC service for this PbftLocal peer.
func RunPbftLocalServer(r *PbftLocal, port int) {
	// Convert port to a string form
	portString := fmt.Sprintf(":%d", port)
	// Create socket that listens on the supplied port
	c, err := net.Listen("tcp", portString)
	if err != nil {
		// Note the use of Fatalf which will exit the program after reporting the error.
		log.Fatalf("Could not create listening socket %v", err)
	}
	// Create a new GRPC server
	s := grpc.NewServer()

	pb.RegisterPbftLocalServer(s, r)
	log.Printf("Going to listen on port %v", port)

	// Start serving, this will block this function and only return when done.
	if err := s.Serve(c); err != nil {
		log.Fatalf("Failed to serve %v", err)
	}
}

func RunPbftGlobalServer(r *PbftGlobal, port int) {
	// Convert port to a string form
	portString := fmt.Sprintf(":%d", port)
	// Create socket that listens on the supplied port
	c, err := net.Listen("tcp", portString)
	if err != nil {
		// Note the use of Fatalf which will exit the program after reporting the error.
		log.Fatalf("Could not create listening socket %v", err)
	}
	// Create a new GRPC server
	s := grpc.NewServer()

	pb.RegisterPbftGlobalServer(s, r)
	log.Printf("Going to listen on port %v", port)

	// Start serving, this will block this function and only return when done.
	if err := s.Serve(c); err != nil {
		log.Fatalf("Failed to serve %v", err)
	}
}




func ConnectToPeer(peer string) (pb.PbftLocalClient, error) {
	backoffConfig := grpc.DefaultBackoffConfig
	// Choose an aggressive backoff strategy here.
	backoffConfig.MaxDelay = 500 * time.Millisecond
	conn, err := grpc.Dial(peer, grpc.WithInsecure(), grpc.WithBackoffConfig(backoffConfig))
	// Ensure connection did not fail, which should not happen since this happens in the background
	if err != nil {
		return pb.NewPbftLocalClient(nil), err
	}
	return pb.NewPbftLocalClient(conn), nil
}


func ConnectToClient(client string) (pb.PbftGlobalClient, error) {
	backoffConfig := grpc.DefaultBackoffConfig
	// Choose an aggressive backoff strategy here.
	backoffConfig.MaxDelay = 500 * time.Millisecond
	conn, err := grpc.Dial(client, grpc.WithInsecure(), grpc.WithBackoffConfig(backoffConfig))
	// Ensure connection did not fail, which should not happen since this happens in the background
	if err != nil {
		return pb.NewPbftGlobalClient(nil), err
	}
	return pb.NewPbftGlobalClient(conn), nil
}


////////////////////debug///////////////////
func PrintLogEntries(local_log []*pb.Entry) {
	// local_logs := ""
	for idx, entry := range local_log {
		// entryLog := "(" + string(entry.Index) + ", " + string(entry.Term) + ")"
		ecmd := ""
		log.Printf("local logs - ")	
		switch c := entry.Cmd; 
		c.Operation {
		case pb.Op_GET:
			ecmd = "Op_GET"
		case pb.Op_SET:
			ecmd = "Op_SET"
		case pb.Op_CLEAR:
			ecmd = "Op_CLEAR"
		case pb.Op_CAS:
			ecmd = "Op_CAS"
		}
		log.Printf("idx %v log : Index %v Term %v Cmd %v", idx, entry.Index, entry.Term, ecmd)	
		// local_logs = entryLog + " " + local_logs
	}
}
