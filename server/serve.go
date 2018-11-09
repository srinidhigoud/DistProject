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

// Messages that can be passed from the Raft RPC server to the main loop for AppendEntries
type AppendEntriesInput struct {
	arg      *pb.AppendEntriesArgs
	response chan pb.AppendEntriesRet
}

// Messages that can be passed from the Raft RPC server to the main loop for VoteInput
type VoteInput struct {
	arg      *pb.RequestVoteArgs
	response chan pb.RequestVoteRet
}

// Struct off of which we shall hang the Raft service
type Raft struct {
	AppendChan chan AppendEntriesInput
	VoteChan   chan VoteInput
}

func (r *Raft) AppendEntries(ctx context.Context, arg *pb.AppendEntriesArgs) (*pb.AppendEntriesRet, error) {
	c := make(chan pb.AppendEntriesRet)
	r.AppendChan <- AppendEntriesInput{arg: arg, response: c}
	result := <-c
	return &result, nil
}

func (r *Raft) RequestVote(ctx context.Context, arg *pb.RequestVoteArgs) (*pb.RequestVoteRet, error) {
	c := make(chan pb.RequestVoteRet)
	r.VoteChan <- VoteInput{arg: arg, response: c}
	result := <-c
	return &result, nil
}

// Compute a random duration in milliseconds
func randomDuration(r *rand.Rand, heartbeat bool) time.Duration {
	// Constant
	if heartbeat{
		const DurationMax = 4000
		const DurationMin = 1000
		return time.Duration(r.Intn(DurationMax-DurationMin)+DurationMin) * time.Millisecond
	} else {
		const DurationMax = 400
		const DurationMin = 100
		return time.Duration(r.Intn(DurationMax-DurationMin)+DurationMin) * time.Millisecond
	}
	
}


// Restart the supplied timer using a random timeout based on function above
func restartTimer(timer *time.Timer, r *rand.Rand, heartbeat bool) {
	stopped := timer.Stop()
	// If stopped is false that means someone stopped before us, which could be due to the timer going off before this,
	// in which case we just drain notifications.
	if !stopped {
		// Loop for any queued notifications
		for len(timer.C) > 0 {
			<-timer.C
		}

	}
	timer.Reset(randomDuration(r, heartbeat))
}



// Launch a GRPC service for this Raft peer.
func RunRaftServer(r *Raft, port int) {
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

	pb.RegisterRaftServer(s, r)
	log.Printf("Going to listen on port %v", port)

	// Start serving, this will block this function and only return when done.
	if err := s.Serve(c); err != nil {
		log.Fatalf("Failed to serve %v", err)
	}
}

func connectToPeer(peer string) (pb.RaftClient, error) {
	backoffConfig := grpc.DefaultBackoffConfig
	// Choose an aggressive backoff strategy here.
	backoffConfig.MaxDelay = 500 * time.Millisecond
	conn, err := grpc.Dial(peer, grpc.WithInsecure(), grpc.WithBackoffConfig(backoffConfig))
	// Ensure connection did not fail, which should not happen since this happens in the background
	if err != nil {
		return pb.NewRaftClient(nil), err
	}
	return pb.NewRaftClient(conn), nil
}


// The main service loop. All modifications to the KV store are run through here.
func serve(s *KVStore, r *rand.Rand, peers *arrayPeers, id string, port int) {
	const MaxUint = ^uint64(0) 
	const MinUint = 0 
	const MaxInt = int64(MaxUint >> 1) 
	const MinInt = -MaxInt - 1
	raft := Raft{AppendChan: make(chan AppendEntriesInput), VoteChan: make(chan VoteInput)}
	// Start in a Go routine so it doesn't affect us.
	go RunRaftServer(&raft, port)
	peerClients := make(map[string]pb.RaftClient)
	//////////////////////////////////////////////////////////////STATE////////////////////////////////////////////////////////////
	
	peer_count := len(*peers)+1//here///////////////////////
	timer := time.NewTimer(randomDuration(r, false))
	myState := "1"
	votedFor := ""
	vote_count := 0
	myLeaderID := ""
	var myLog []*pb.Entry
	currentTerm := int64(0)
	myLastLogTerm := int64(0)
	myLastLogIndex := int64(0)
	//1-follower, 2-Candidate, 3-Leader
	
	

	// Leader Stuff
	// isLeader := false
	// leaderCommit := int64(0)
	// leaderLogTerm := int64(0)
	// leaderLogIndex := int64(0)
	
	// Persistent state on all servers:
	
	
	// var opEntries []chan pb.Result
	
	// Volatile state on all servers:
	myCommitIndex := int64(0)
	myLastApplied := int64(0)

	// Volatile state on leaders:
	// myNextIndex := []
	myNextIndex := make(map[string]int64)
	myMatchIndex := make(map[string]int64)
	clientReq_id_map := make(map[int64]InputChannelType)
	// myMatchIndex := []

	// Candidate args:
	// candidateTerm := int64(0);
    // candidateID := "";
    // candidateLastLogIndex := int64(0);
	// candidateLasLogTerm := int64(0);
	for _, peer := range *peers {
		client, err := connectToPeer(peer)
		if err != nil {
			log.Fatalf("Failed to connect to GRPC server %v", err)
		}

		peerClients[peer] = client
		log.Printf("Connected to %v", peer)
	}

	type AppendResponse struct {
		ret  *pb.AppendEntriesRet
		err  error
		peer string
		len_ae int64 // length of append tries
	}

	type VoteResponse struct {
		ret  *pb.RequestVoteRet
		err  error
		peer string
	}
	appendResponseChan := make(chan AppendResponse)
	voteResponseChan := make(chan VoteResponse)

	


 	count_inf := int64(0)	
	// Run forever handling inputs from various channels
	for {
		if count_inf%1000 == 0 {
		//	log.Printf("%v",count_inf)
		}
		count_inf += 1
		select {
				
			case <-timer.C:
				log.Printf("Timeout wentoff")
				// The timer went off.
				if myState != "3" {
					log.Printf("Timeout %v", id)
					currentTerm += 1
					vote_count = 1
					numberOfPeers := len(peerClients)///chekc here
					myState = "2"
					for p, c := range peerClients {
						// Send in parallel so we don't wait for each client.
						go func(c pb.RaftClient, p string) {
							// ret, err := c.RequestVote(context.Background(), &pb.RequestVoteArgs{Term: 1, CandidateID: id})
							ret, err := c.RequestVote(context.Background(), &pb.RequestVoteArgs{Term: currentTerm, CandidateID: id, LastLogIndex: myLastLogIndex, LasLogTerm: myLastLogTerm})
							voteResponseChan <- VoteResponse{ret: ret, err: err, peer: p}
						}(c, p)
						// numberOfPeers += 1
					}
					log.Printf("I'm a candidate %v - sent to %v peers", id, numberOfPeers)
					restartTimer(timer, r, false)
				} 
				// else {
				// 	// Send heartbeats
				// 	heartbeat := pb.AppendEntriesArgs{Term: currentTerm, LeaderID: id, PrevLogIndex: myLastLogIndex, PrevLogTerm: myLastLogTerm, LeaderCommit: myCommitIndex}
				// 	for p, c := range peerClients {
				// 		go func(c pb.RaftClient, p string) {
				// 			ret, err := c.AppendEntries(context.Background(), &heartbeat)
				// 			appendResponseChan <- AppendResponse{ret: ret, err: err, peer: p, len_ae: int64(0)}
				// 		}(c, p)
				// 	}
				// 	restartTimer(timer, r, true)
				// }
				// This will also take care of any pesky timeouts that happened while processing the operation.
				
			case op := <-s.C:
				log.Printf("We received an operation from a client")
				// We received an operation from a client
				// TODO: Figure out if you can actually handle the request here. If not use the Redirect result to send the
				// client elsewhere.
				// TODO: Use Raft to make sure it is safe to actually run the command.

				if myState == "3" {
					new_entry := pb.Entry{Term: currentTerm, Index: myLastLogIndex + 1, Cmd: &op.command}
					myLog = append(myLog, &new_entry)
					new_entry_list := []*pb.Entry{&new_entry}
					clientReq_id_map[new_entry.Index] = op
					// cannot use &newEntries (type *[]pb.Entry) as type []*pb.Entry in field value // how to deal with this?
					appendEntry := pb.AppendEntriesArgs{Term: currentTerm, LeaderID: id, PrevLogIndex: myLastLogIndex, PrevLogTerm: myLastLogTerm, LeaderCommit: myCommitIndex, Entries: new_entry_list}
					for p, c := range peerClients {
						go func(c pb.RaftClient, p string) {
							ret, err := c.AppendEntries(context.Background(), &appendEntry)
							appendResponseChan <- AppendResponse{ret: ret, err: err, peer: p, len_ae: int64(len(new_entry_list))}
						}(c, p)
					}
					myLastLogIndex = myLastLogIndex + int64(len(new_entry_list)) // here?
				} else {
					// 	Reply with most recent leader's address // 
					res := pb.Result{Result: &pb.Result_Redirect{Redirect: &pb.Redirect{Server: myLeaderID}}}
					op.response <- res
					log.Printf("Redirect to client")
				}

				// s.HandleCommand(op) //- last command?
			case ae := <-raft.AppendChan:
				log.Printf("We received an AppendEntries request from a Raft peer")
				// We received an AppendEntries request from a Raft peer
				// TODO figure out what to do here, what we do is entirely wrong.
				// Can change to follower here as well from candidate
				leaderCommit := ae.arg.LeaderCommit
				leaderPrevLogIndex := ae.arg.PrevLogIndex
				leaderPrevLogTerm := ae.arg.PrevLogTerm
				ae_list := ae.arg.Entries
				isHeartBeat := false
				if len(ae_list) == 0 {
					isHeartBeat = true  
				} 
				
				if isHeartBeat {
					log.Printf("Received heartbeat from %v", myLeaderID)
					if ae.arg.Term > currentTerm {
						currentTerm = ae.arg.Term
						myState = "1"
						myLeaderID = ae.arg.LeaderID
						log.Printf("All hail new leader %v in term %v (heartbeat)", myLeaderID,currentTerm)
					}
					// Otherwise disregard
				} else {
					log.Printf("Received append entry from %v", ae.arg.LeaderID)
					if ae.arg.Term < currentTerm {
						ae.response <- pb.AppendEntriesRet{Term: currentTerm, Success: false}
					} else {
						if myLastLogIndex == leaderPrevLogIndex  && myLog[myLastLogIndex].Term == leaderPrevLogTerm {
							for _, entry := range ae_list {
								myLog = append( myLog, entry)
							}
							
							ae.response <- pb.AppendEntriesRet{Term: currentTerm, Success: true}
						} else {
							if myLastLogIndex < leaderPrevLogIndex || myLog[myLastLogIndex].Term != leaderPrevLogTerm {
								ae.response <- pb.AppendEntriesRet{Term: currentTerm, Success: false}
							} else {
								if myLastLogIndex > leaderPrevLogIndex || myLog[myLastLogIndex].Term != leaderPrevLogTerm{
									min_index := MaxInt
									for _, entry := range ae_list {
										if myLog[entry.Index].Term != entry.Term {
											// delete everything after it
											if entry.Index < min_index {
												min_index = entry.Index
											}
											
										}
									}
									myLog = myLog[:min_index]
								}
							}
						}
						currentTerm = ae.arg.Term // ?? here ??
						myState = "1" // ??
						myLeaderID = ae.arg.LeaderID // ?? here ??
						myLastLogIndex = int64(len(myLog) - 1)
						myLastLogTerm = myLog[myLastLogIndex].Term
						if leaderCommit < myLastLogIndex {
							myCommitIndex = leaderCommit
						} else {
							myCommitIndex = myLastLogIndex
						}
					}
				}

				// ae.response <- pb.AppendEntriesRet{Term: currentTerm, Success: true}
				// s.HandleCommand(op) - // here?
				// This will also take care of any pesky timeouts that happened while processing the operation.
				restartTimer(timer, r, false)
			case vr := <-raft.VoteChan:
				//log.Printf("We received a RequestVote RPC from a raft peer")
				// We received a RequestVote RPC from a raft peer
				// TODO: Fix this.

				candidateTerm := vr.arg.Term
				candidateID := vr.arg.CandidateID
				candidateLastLogIndex := vr.arg.LastLogIndex
				candidateLasLogTerm := vr.arg.LasLogTerm
				suc := false

				if candidateTerm < currentTerm {
					vr.response <- pb.RequestVoteRet{Term: currentTerm, VoteGranted: false}
				} else {
					if candidateTerm > currentTerm {
						votedFor = ""
					}
					currentTerm = candidateTerm
				}
				if votedFor == "" || votedFor == candidateID {
					if candidateLasLogTerm > myLastLogTerm || candidateLastLogIndex >= myLastLogIndex {
						vr.response <- pb.RequestVoteRet{Term: currentTerm, VoteGranted: true}
						votedFor = candidateID
						suc = true
					} else {
						vr.response <- pb.RequestVoteRet{Term: currentTerm, VoteGranted: false}
					}
				} else {
					vr.response <- pb.RequestVoteRet{Term: currentTerm, VoteGranted: false}
				}
				
				myState = "1"
				restartTimer(timer, r, false) // ??

				log.Printf("Received vote request from %v", vr.arg.CandidateID)
				if suc {
					log.Printf("I am follower %v -  votedFor %v", id, votedFor)
					myLeaderID = votedFor
				}
				log.Printf("We received a RequestVote RPC from a raft peer")
				// vr.response <- pb.RequestVoteRet{Term: currentTerm, VoteGranted: false} // Should it be last call?
			case vr := <-voteResponseChan:
				// We received a response to a previou vote request.
				log.Printf("We received a response to a previou vote request.")
				// TODO: Fix this
				if vr.err != nil {
					// Do not do Fatalf here since the peer might be gone but we should survive.
					log.Printf("Error calling RPC %v", vr.err)
				} else {
					// peerID := vr.peer
					peerVoteGranted := vr.ret.VoteGranted
					peerTerm := vr.ret.Term
					if peerTerm > currentTerm {
						log.Printf("Stepping down to follower %v - received response of term %v, greater than my term %v", id, peerTerm, currentTerm)
						myState = "1"
						currentTerm = peerTerm
					} else {
						if peerVoteGranted {
							vote_count += 1
							if vote_count > peer_count/2 && myState == "2" {
								// isLeader = true
								// vote_count = 0
								myState = "3"
								myLeaderID = id
								log.Printf("I am leader %v term %v - got %v votes out of %v", id, peerTerm, vote_count, peer_count)
								// initialize myNextIndex and update myMatchIndex maybe?
								// send heartbeat here? Or empty messages
								// Multicast empty AppendEntries here
								heartbeat := pb.AppendEntriesArgs{Term: currentTerm, LeaderID: id, PrevLogIndex: myLastLogIndex, PrevLogTerm: myLastLogTerm, LeaderCommit: myCommitIndex}
								for p, c := range peerClients {
									myNextIndex[p] = myLastLogIndex + 1
									go func(c pb.RaftClient, p string) {
										ret, err := c.AppendEntries(context.Background(), &heartbeat)
										appendResponseChan <- AppendResponse{ret: ret, err: err, peer: p, len_ae: int64(0)}
									}(c, p)
								}
								// break //?
								restartTimer(timer, r, true)
							}
						}
					}	

					log.Printf("Got response to vote request from %v", vr.peer)
					log.Printf("Peers %s granted %v term %v", vr.peer, vr.ret.VoteGranted, vr.ret.Term)
				}
			case ar := <-appendResponseChan:
				log.Printf("We received a response to a previous AppendEntries RPC call")
				// We received a response to a previous AppendEntries RPC call
				follower := ar.peer
				// followerTerm := ar.ret.Term
				followerAppendSuccess := ar.ret.Success // For decrementing myNextIndex and retrying
				lenOfAppendedEntries := ar.len_ae
				// operation := ar.oper
				if ar.err != nil {
					// Do not do Fatalf here since the peer might be gone but we should survive.
					log.Printf("Error calling RPC %v", ar.err)
					// keep retrying here - a failed follower node
					retryLastLogIndex := myLog[myNextIndex[follower]].Index
					retryLastLogTerm := myLog[myNextIndex[follower]].Term
					replacingPlusNewEntries := myLog[myNextIndex[follower]:]
					retryAppendEntry := pb.AppendEntriesArgs{Term: currentTerm, LeaderID: id, PrevLogIndex: retryLastLogIndex, PrevLogTerm: retryLastLogTerm, LeaderCommit: myCommitIndex, Entries: replacingPlusNewEntries}

					go func(c pb.RaftClient, p string) {
						ret, err := c.AppendEntries(context.Background(), &retryAppendEntry)
						appendResponseChan <- AppendResponse{ret: ret, err: err, peer: p, len_ae: int64(len(replacingPlusNewEntries))}
					}(peerClients[follower], follower)

				} else {
					if followerAppendSuccess {
						// what the fuck? update myNextIndex and myMatchIndex
						myMatchIndex[follower] = myLog[myNextIndex[follower]].Index + int64(lenOfAppendedEntries)
						// Find a way to not add redundant entries' lengths

						// myNextIndex update how?
						myNextIndex[follower] = myMatchIndex[follower] + 1

						// If there exists an N such that N > myCommitIndex, a majority
						// of myMatchIndex[i] ≥ N, and log[N].term == currentTerm: set 
						// myCommitIndex = N (§5.3, §5.4).	
						nextMaxmyCommitIndex := myCommitIndex
						for i := myCommitIndex; i <= myLastLogIndex; i++ {
							peer_countReplicatedUptoi := 0
							for _, followermyMatchIndex := range myMatchIndex {
								if followermyMatchIndex >= i {
									peer_countReplicatedUptoi += 1
								}
							}
							if peer_countReplicatedUptoi > peer_count/2 {
								nextMaxmyCommitIndex = i
							}
						}
						myCommitIndex = nextMaxmyCommitIndex

					} else {
						myNextIndex[follower] -= 1
						retryLastLogIndex := myLog[myNextIndex[follower]].Index
						retryLastLogTerm := myLog[myNextIndex[follower]].Term
						replacingPlusNewEntries := myLog[myNextIndex[follower]:]
						retryAppendEntry := pb.AppendEntriesArgs{Term: currentTerm, LeaderID: id, PrevLogIndex: retryLastLogIndex, PrevLogTerm: retryLastLogTerm, LeaderCommit: myCommitIndex, Entries: replacingPlusNewEntries}

						go func(c pb.RaftClient, p string) {
							ret, err := c.AppendEntries(context.Background(), &retryAppendEntry)
							appendResponseChan <- AppendResponse{ret: ret, err: err, peer: p, len_ae: int64(len(replacingPlusNewEntries))}
						}(peerClients[follower], follower)
					}	
				}

				// Check number of true responses and commit here ??
				// s.HandleCommand(op)????
				log.Printf("Got append entries response from %v", ar.peer)

			default:
				//log.Printf("Default")
				// Apply here ??? If not leader maybe ?
				if myCommitIndex > myLastApplied {
					
					// toApply := myLog[myLastApplied]
					// opCmd := toApply.Cmd // ??
					clientRequest, existsInMyMachine := clientReq_id_map[myLastApplied]
					if myState == "3" {
						if existsInMyMachine {
							// To handle unwanted cases
							s.HandleCommand(clientRequest, true) 
						}
					} else {
						s.HandleCommand(clientRequest, false)  // This one just executes it on its own machine
					}
					myLastApplied += 1
				}
				// When to send back to client? 
				// Where do we keep trying a failed node as a leader?
		}
	}
	log.Printf("Strange to arrive here")
}

