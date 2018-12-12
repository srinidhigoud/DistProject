package util

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	rand "math/rand"
	"net"
	"time"

	context "golang.org/x/net/context"
	// "context"

	"google.golang.org/grpc"

	"github.com/nyu-distributed-systems-fa18/DistProject/pb"
	// "DistProject/pb"
)

// Messages that can be passed from the Pbft RPC server to the main loop for AppendEntries

// type PrePrepareMsgInput struct {
// 	Arg *pb.PrePrepareMsg
// }

type PbftMsgInput struct {
	Arg *pb.Msg
}

// type PrepareMsgInput struct {
// 	Arg *pb.PrepareMsg
// }

// type CommitMsgInput struct {
// 	Arg *pb.CommitMsg
// }

// type ViewChangeMsgInput struct {
// 	Arg *pb.ViewChangeMsg
// }
type Pbft struct {
	PbftMsgChan chan PbftMsgInput
	// PrepareMsgChan    chan PrepareMsgInput
	// CommitMsgChan     chan CommitMsgInput
	// ViewChangeMsgChan chan ViewChangeMsgInput
	// ResponseChan      chan *pb.ClientResponse
}

func (r *Pbft) SendPbftMsg(ctx context.Context, arg *pb.Msg) (*pb.Success, error) {
	r.PbftMsgChan <- PbftMsgInput{Arg: arg}
	result := pb.Success{}
	return &result, nil
}

// func (r *Pbft) PreparePBFT(ctx context.Context, arg *pb.PrepareMsg) (*pb.Success, error) {
// 	r.PrepareMsgChan <- PrepareMsgInput{Arg: arg}
// 	result := pb.Success{}
// 	return &result, nil
// }

// func (r *Pbft) CommitPBFT(ctx context.Context, arg *pb.CommitMsg) (*pb.Success, error) {
// 	r.CommitMsgChan <- CommitMsgInput{Arg: arg}
// 	result := pb.Success{}
// 	return &result, nil
// }

// func (r *Pbft) ViewChangePBFT(ctx context.Context, arg *pb.ViewChangeMsg) (*pb.Success, error) {
// 	r.ViewChangeMsgChan <- ViewChangeMsgInput{Arg: arg}
// 	result := pb.Success{}
// 	return &result, nil
// }

// func (r *Pbft) SendResponseBack(ctx context.Context, Arg *pb.ClientResponse) (*pb.Success, error) {
// 	r.ResponseChan <- Arg
// 	result := pb.Success{}
// 	return &result, nil

// }

func RandomDuration(r *rand.Rand) time.Duration {
	const DurationMax = 4000
	const DurationMin = 1000
	return time.Duration(r.Intn(DurationMax-DurationMin)+DurationMin) * time.Millisecond
}

func RandomDuration2(r *rand.Rand) time.Duration {
	const DurationMax = 8000
	const DurationMin = 1000
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

type SecondsTimer struct {
	Timer *time.Timer
	End   time.Time
}

func NewSecondsTimer(t time.Duration) *SecondsTimer {
	return &SecondsTimer{time.NewTimer(t), time.Now().Add(t)}
}

func (s *SecondsTimer) Reset(t time.Duration) {
	stopped := s.Timer.Stop()
	if !stopped {
		for len(s.Timer.C) > 0 {
			<-s.Timer.C
		}
	}
	s.Timer.Reset(t)
	s.End = time.Now().Add(t)
}

func (s *SecondsTimer) Stop() {
	stopped := s.Timer.Stop()
	if !stopped {
		for len(s.Timer.C) > 0 {
			<-s.Timer.C
		}
	}
	s.Timer.Stop()
}

func (s *SecondsTimer) TimeRemaining() time.Duration {
	return s.End.Sub(time.Now())
}

func RunPbftServer(r *Pbft, port int) {
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

	pb.RegisterPbftServer(s, r)
	log.Printf("Going to listen on port %v", port)

	// Start serving, this will block this function and only return when done.
	if err := s.Serve(c); err != nil {
		log.Fatalf("Failed to serve %v", err)
	}
}

func ConnectToPeer(peer string) (pb.PbftClient, error) {
	backoffConfig := grpc.DefaultBackoffConfig
	// Choose an aggressive backoff strategy here.
	backoffConfig.MaxDelay = 500 * time.Millisecond
	conn, err := grpc.Dial(peer, grpc.WithInsecure(), grpc.WithBackoffConfig(backoffConfig))
	// Ensure connection did not fail, which should not happen since this happens in the background
	if err != nil {
		return pb.NewPbftClient(nil), err
	}
	return pb.NewPbftClient(conn), nil
}

func ConnectToClient(client string) (pb.PbftClient, error) {
	backoffConfig := grpc.DefaultBackoffConfig
	// Choose an aggressive backoff strategy here.
	backoffConfig.MaxDelay = 500 * time.Millisecond
	conn, err := grpc.Dial(client, grpc.WithInsecure(), grpc.WithBackoffConfig(backoffConfig))
	// Ensure connection did not fail, which should not happen since this happens in the background
	if err != nil {
		return pb.NewPbftClient(nil), err
	}
	return pb.NewPbftClient(conn), nil
}

////////////////////debug///////////////////
// func PrintLogEntries(local_log []*pb.Entry) {
// 	// local_logs := ""
// 	for idx, entry := range local_log {
// 		// entryLog := "(" + string(entry.Index) + ", " + string(entry.Term) + ")"
// 		ecmd := ""
// 		log.Printf("local logs - ")
// 		switch c := entry.Cmd; c.Operation {
// 		case pb.Op_GET:
// 			ecmd = "Op_GET"
// 		case pb.Op_SET:
// 			ecmd = "Op_SET"
// 		case pb.Op_CLEAR:
// 			ecmd = "Op_CLEAR"
// 		case pb.Op_CAS:
// 			ecmd = "Op_CAS"
// 		}
// 		log.Printf("idx %v log : Index %v Term %v Cmd %v", idx, entry.Index, entry.Term, ecmd)
// 		// local_logs = entryLog + " " + local_logs
// 	}
// }

func Hash(content []byte) string {
	h := sha256.New()
	h.Write(content)
	return hex.EncodeToString(h.Sum(nil))
}

func Digest(object interface{}) string {
	msg, err := json.Marshal(object)
	if err != nil {
		// return "", err
		log.Fatal("Cannot make digest")
	}
	return Hash(msg)
}
