package main

import (
	// "fmt"
	"log"
	rand "math/rand"
	"strconv"

	// "net"
	"time"

	context "golang.org/x/net/context"

	"github.com/nyu-distributed-systems-fa18/DistProject/pb"
	"github.com/nyu-distributed-systems-fa18/DistProject/util"
	// "DistProject/pb"
	// "DistProject/util"
	// "context"
)

//serve(&store, r, &peers, &clients, id, pbftPort)

// The main service loop. All modifications to the KV store are run through here.
// func serve(s *KVStore, r *rand.Rand, peers *util.ArrayPeers, id string, port int) {
func printClientRequest(cr pb.ClientRequest, view int64, seq int64) {
	log.Printf("Received client request in view %v and seq %v", view, seq)
	log.Printf("ClientRequest - operation : %v || timestamp : %v || clientId : %v", cr.Cmd.Operation, cr.Timestamp, cr.ClientID)
}

func printClientResponse(cr pb.ClientResponse, view int64, seq int64) {
	log.Printf("Sending client response in view %v and seq %v", view, seq)
	log.Printf("ClientResponse - viewId : %v || timestamp : %v || clientId : %v || node : %v || nodeResult : %v", cr.ViewId, cr.Timestamp, cr.ClientID, cr.Node, cr.NodeResult)
}

func printPrePrepareMsg(ppm pb.PrePrepareMsg, view int64, seq int64) {
	log.Printf("Received pre-prepare request in view %v and seq %v", view, seq)
	log.Printf("PrePrepareMsg - viewId : %v || sequenceID : %v || digest : %v || request : %v || node : %v", ppm.ViewId, ppm.SequenceID, ppm.Digest, ppm.Request, ppm.Node)
}

func printPrepareMsg(pm pb.PrepareMsg, view int64, seq int64) {
	log.Printf("Received prepare request in view %v and seq %v", view, seq)
	log.Printf("PrepareMsg - viewId : %v || sequenceID : %v || digest : %v || node : %v", pm.ViewId, pm.SequenceID, pm.Digest, pm.Node)
}

func printCommitMsg(cm pb.CommitMsg, view int64, seq int64) {
	log.Printf("Received commit request in view %v and seq %v", view, seq)
	log.Printf("CommitMsg - viewId : %v || sequenceID : %v || digest : %v || node : %v", cm.ViewId, cm.SequenceID, cm.Digest, cm.Node)
}

func printPbftMsgAccepted(pma pb.PbftMsgAccepted, view int64, seq int64) {
	log.Printf("Received msg acc in view %v and seq %v", view, seq)
	log.Printf("PbftMsgAccepted - viewId : %v || sequenceID : %v || success : %v || typeOfAccepted : %v || node : %v", pma.ViewId, pma.SequenceID, pma.Success, pma.TypeOfAccepted, pma.Node)
}

type logEntry struct {
	viewId         int64
	sequenceID     int64
	clientReq      *pb.ClientRequest
	prePrep        *pb.PrePrepareMsg
	pre            []*pb.PrepareMsg
	com            []*pb.CommitMsg
	prepared       bool
	committed      bool
	committedLocal bool
}

func reqValidPrepare(n int64) int64 {
	f := (n - 1) / 3
	return 2 * f
}

func reqValidCommit(n int64) int64 {
	f := (n - 1) / 3
	return 1 + f
}

func reqValidCommitLocal(n int64) int64 {
	f := (n - 1) / 3
	return 2*f + 1
}

func reqValidVC(n int64) int64 {
	f := (n - 1) / 3
	log.Printf("%v", f)
	return 2 * f
}

func verifyPrePrepare(prePreMsg *pb.PrePrepareMsg, viewId int64, sequenceID int64, logEntries []logEntry) bool {

	if prePreMsg.ViewId != viewId {
		log.Printf("here1")
		return false
	}
	if sequenceID != -1 {
		if sequenceID > prePreMsg.SequenceID {
			log.Printf("here2")
			return false
		}
	}
	digest := util.Digest(prePreMsg.Request)
	if digest != prePreMsg.Digest {
		log.Printf("here3")
		return false
	}
	return true
}

func verifyPrepare(prepareMsg *pb.PrepareMsg, viewId int64, sequenceID int64, logEntries []logEntry) bool {
	if prepareMsg.ViewId != viewId {
		return false
	}
	if sequenceID != -1 {
		if sequenceID > prepareMsg.SequenceID {
			return false
		}
	}
	if prepareMsg.SequenceID+1 > int64(len(logEntries)) {
		return false
	}
	digest := logEntries[prepareMsg.SequenceID].prePrep.Digest
	if digest != prepareMsg.Digest {
		return false
	}
	return true
}

func verifyCommit(commitMsg *pb.CommitMsg, viewId int64, sequenceID int64, logEntries []logEntry) bool {
	if commitMsg.ViewId != viewId {
		return false
	}
	if sequenceID != -1 {
		if sequenceID > commitMsg.SequenceID {
			return false
		}
	}
	log.Printf("The log size is %v, the seq id is %v", len(logEntries), commitMsg.SequenceID)
	digest := logEntries[commitMsg.SequenceID].prePrep.Digest
	if digest != commitMsg.Digest {
		return false
	}
	return true
}

func isPrepared(entry logEntry, n int64) bool {
	prePreMsg := entry.prePrep
	if prePreMsg != nil {
		validPrepares := int64(0)
		for i := 0; i < len(entry.pre); i++ {
			prepareMsg := entry.pre[i]
			if prepareMsg.ViewId == prePreMsg.ViewId && prepareMsg.SequenceID == prePreMsg.SequenceID && prepareMsg.Digest == prePreMsg.Digest {
				validPrepares += 1
			}
		}
		return validPrepares >= reqValidPrepare(n)
	}
	return false
}

func isCommitted(entry logEntry, n int64) bool {
	prepared := isPrepared(entry, n)
	if prepared {
		prePreMsg := entry.prePrep
		validCommits := int64(0)
		for i := 0; i < len(entry.com); i++ {
			commitMsg := entry.com[i]
			if commitMsg.ViewId == prePreMsg.ViewId && commitMsg.SequenceID == prePreMsg.SequenceID && commitMsg.Digest == prePreMsg.Digest {
				validCommits += 1
			}
		}
		return validCommits >= reqValidCommit(n)
	}
	return false
}

func isCommittedLocal(entry logEntry, n int64) bool {
	committed := isCommitted(entry, n)
	if committed {
		prePreMsg := entry.prePrep
		validCommits := int64(0)
		for i := 0; i < len(entry.com); i++ {
			commitMsg := entry.com[i]
			if commitMsg.ViewId == prePreMsg.ViewId && commitMsg.SequenceID == prePreMsg.SequenceID && commitMsg.Digest == prePreMsg.Digest {
				validCommits += 1
			}
		}
		return validCommits >= reqValidCommitLocal(n)
	}
	return false
}

func printMyStoreAndLog(logEntries []logEntry, s *KVStore, currentView int64, curreSeqID int64) {
	log.Printf("currentView - %v", currentView)
	log.Printf("My KVStore - %v", s.store)
	log.Printf("My Logs - %v", logEntries)
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandStringRunes(n int64) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func tamper(digest string) string {
	return RandStringRunes(int64(len(digest)))
}

type ClientResponse struct {
	ret  *pb.ClientResponse
	err  error
	node string
}

type PbftMsgAccepted struct {
	ret  *pb.PbftMsgAccepted
	err  error
	peer string
}

//func serve(s *KVStore, r *rand.Rand, peers *util.ArrayPeers, id string, port int) {
func serve(s *KVStore, r *rand.Rand, peers *util.ArrayPeers, id string, port int, isByzantine bool) {
	pbft := util.Pbft{PrePrepareMsgChan: make(chan util.PrePrepareMsgInput), PrepareMsgChan: make(chan util.PrepareMsgInput), CommitMsgChan: make(chan util.CommitMsgInput), ViewChangeMsgChan: make(chan util.ViewChangeMsgInput), ResponseChan: make(chan *pb.ClientResponse)}
	go util.RunPbftServer(&pbft, port)
	peerClients := make(map[string]pb.PbftClient)
	numberOfPeers := int64(1)
	for _, peer := range *peers {
		numberOfPeers += 1
		log.Printf("peer address - %v", peer)
		clientPeer, err := util.ConnectToPeer(peer)
		if err != nil {
			log.Fatalf("Failed to connect to GRPC server %v", err)
		}
		peerClients[peer] = clientPeer
		log.Printf("Connected to %v", peer)
	}

	// pbftMsgAcceptedChan := make(chan PbftMsgAccepted)

	currentView := int64(0)
	curreSeqID := int64(-1)
	var logEntries []logEntry
	msgLimit := 0
	nodeID := int64(port) % 3001
	vcTimer := util.NewSecondsTimer(util.RandomDuration(r))
	vcTimer.Stop()

	viewChangePhase := false
	numberOfVotes := int64(0)

	for {
		select {
		case inpChannel := <-s.C:
			cr := inpChannel.clientRequest
			if !viewChangePhase {
				if currentView == nodeID {
					log.Printf("Received ClientRequestChan %v", cr.ClientID)
					printClientRequest(*cr, currentView, curreSeqID)
					curreSeqID += 1
					digest := util.Digest(cr)
					if isByzantine {
						digest = tamper(digest)
					}
					prePreMsg := pb.PrePrepareMsg{ViewId: currentView, SequenceID: curreSeqID, Digest: digest, Request: cr, Node: id}
					for p, c := range peerClients {
						go func(c pb.PbftClient, p string) {
							_, _ = c.PrePreparePBFT(context.Background(), &prePreMsg)
						}(c, p)
					}
					newEntry := logEntry{viewId: currentView, sequenceID: curreSeqID, clientReq: cr, prePrep: &prePreMsg,
						pre: make([]*pb.PrepareMsg, msgLimit), com: make([]*pb.CommitMsg, msgLimit), prepared: false,
						committed: false, committedLocal: false}
					if int64(len(logEntries)) >= curreSeqID+1 {
						logEntries[curreSeqID] = newEntry
					} else {
						logEntries = append(logEntries, newEntry)
					}
					printMyStoreAndLog(logEntries, s, currentView, curreSeqID)
				} else {
					// Need to send some kind of redirect message
					log.Printf("Send Back Redirect message - View Change")
				}
			} else {
				log.Printf("Received ClientRequestChan %v", cr.ClientID)
				log.Printf("But.....Requested View Change")
			}
		case <-vcTimer.Timer.C:
			newView := (currentView + 1) % numberOfPeers
			log.Printf("Timeout - initiate view change. New view - %v", newView)
			viewChangePhase = true
			viewChange := pb.ViewChangeMsg{Type: "view-change", NewView: newView, LastSequenceID: curreSeqID - 1, Node: id}
			for p, c := range peerClients {
				go func(c pb.PbftClient, p string) {
					_, _ = c.ViewChangePBFT(context.Background(), &viewChange)
				}(c, p)
			}
			printMyStoreAndLog(logEntries, s, currentView, curreSeqID)

		case vc := <-pbft.ViewChangeMsgChan:
			// Got request for vote change
			newView := vc.Arg.NewView
			if vc.Arg.Type == "new-view" {
				log.Printf("Switching to new view - %v || %v", newView, vc)
				// Should send back redirect here for commands that weren't committed
				currentView = newView
				viewChangePhase = false
				numberOfVotes = int64(0)
				curreSeqID = vc.Arg.LastSequenceID

				if currentView == nodeID {
					result := pb.Result{Result: &pb.Result_Redirect{Redirect: &pb.Redirect{Server: strconv.FormatInt(newView+3001, 10)}}}
					clientID := logEntries[len(logEntries)-1].clientReq.ClientID
					client, err := util.ConnectToClient(clientID) //client connection
					if err != nil {
						log.Fatalf("Failed to connect to GRPC server %v", err)
					}
					log.Printf("Connected to %v", clientID)
					go func(c pb.PbftClient) {
						c.SendResponseBack(context.Background(),
							&pb.ClientResponse{ViewId: int64(0), Timestamp: int64(0), ClientID: "", Node: "", NodeResult: &result, SequenceID: int64(-1)})
					}(client)
					log.Printf("Send Back Redirect message - View Change")
				}
			} else {
				if newView == nodeID {
					log.Printf("Received vote from %v", vc.Arg.Node)
					numberOfVotes += 1
				} else {
					log.Printf("Received view change request - %v", vc)
				}
			}
			// New Primary
			if numberOfVotes >= reqValidVC(numberOfPeers) {
				vcTimer.Stop()
				log.Printf("Switching to new view - %v and taking on as primary", newView)
				viewChange := pb.ViewChangeMsg{Type: "new-view", NewView: newView, LastSequenceID: curreSeqID - 1, Node: strconv.FormatInt(nodeID+3001, 10)}
				for p, c := range peerClients {
					go func(c pb.PbftClient, p string) {
						_, _ = c.ViewChangePBFT(context.Background(), &viewChange)
					}(c, p)
				}
				viewChangePhase = false
				currentView = newView
				numberOfVotes = 0
				curreSeqID = vc.Arg.LastSequenceID
			}

		case pp := <-pbft.PrePrepareMsgChan:
			prePreMsg := pp.Arg
			curreSeqID = prePreMsg.SequenceID
			if !viewChangePhase {
				if vcTimer.TimeRemaining() < 100*time.Millisecond {
					dur := util.RandomDuration(r)
					log.Printf("Resetting timer for duration - %v", dur)
					vcTimer.Reset(dur)
				}
				log.Printf("Received PrePrepareMsgChan %v from primary %v", prePreMsg, prePreMsg.Node)
				printPrePrepareMsg(*prePreMsg, currentView, curreSeqID)
				verified := verifyPrePrepare(prePreMsg, currentView, curreSeqID, logEntries)
				if verified {
					digest := prePreMsg.Digest
					if isByzantine {
						digest = tamper(digest)
					}
					prepareMsg := pb.PrepareMsg{ViewId: prePreMsg.ViewId, SequenceID: prePreMsg.SequenceID, Digest: digest, Node: strconv.FormatInt(nodeID+3001, 10)}
					if prePreMsg.SequenceID+1 <= int64(len(logEntries)) {
						log.Printf("Had received a prepare msg before, so writing on previous curreSeqID - %v", prePreMsg.SequenceID)
						oldEntry := logEntries[prePreMsg.SequenceID]
						oldEntry.prePrep = prePreMsg
						oldEntry.clientReq = prePreMsg.Request
						oldPrepares := oldEntry.pre
						oldPrepares = append(oldPrepares, &prepareMsg)
						oldEntry.pre = oldPrepares
						logEntries[prePreMsg.SequenceID] = oldEntry
					} else {
						log.Printf("Appending new entry to log")
						newEntry := logEntry{viewId: prePreMsg.ViewId, sequenceID: prePreMsg.SequenceID, clientReq: prePreMsg.Request, prePrep: prePreMsg, pre: make([]*pb.PrepareMsg, msgLimit), com: make([]*pb.CommitMsg, msgLimit), prepared: false, committed: false, committedLocal: false}
						oldPrepares := newEntry.pre
						oldPrepares = append(oldPrepares, &prepareMsg)
						newEntry.pre = oldPrepares
						logEntries = append(logEntries, newEntry)
					}
					for p, c := range peerClients {
						go func(c pb.PbftClient, p string) {
							_, _ = c.PreparePBFT(context.Background(), &prepareMsg)
						}(c, p)
					}
				}
				printMyStoreAndLog(logEntries, s, currentView, curreSeqID)
			} else {
				log.Printf("Received PrePrepareMsgChan %v from primary %v", prePreMsg, prePreMsg.Node)
				log.Printf("But.....Requested View Change")
				log.Printf("Send Back Redirect message - View Change")
			}
		case p := <-pbft.PrepareMsgChan:
			prepareMsg := p.Arg
			if !viewChangePhase {
				log.Printf("Received PrepareMsgChan %v", prepareMsg)
				printPrepareMsg(*prepareMsg, currentView, curreSeqID)
				verified := verifyPrepare(prepareMsg, currentView, curreSeqID, logEntries)
				if verified {
					if vcTimer.TimeRemaining() < 100*time.Millisecond {
						dur := util.RandomDuration(r)
						log.Printf("Resetting timer for duration - %v", dur)
						vcTimer.Reset(dur)
					}
					prepared := false
					if prepareMsg.SequenceID+1 <= int64(len(logEntries)) {
						log.Printf("Normal case received pre-prepare before prepare - writing to entry in logs at - %v", prepareMsg.SequenceID)
						oldEntry := logEntries[prepareMsg.SequenceID]
						oldPrepares := oldEntry.pre
						oldPrepares = append(oldPrepares, prepareMsg)
						oldEntry.pre = oldPrepares
						logEntries[prepareMsg.SequenceID] = oldEntry
						prepared = isPrepared(oldEntry, numberOfPeers)
						oldEntry.prepared = prepared
						logEntries[prepareMsg.SequenceID] = oldEntry
					} else {
						log.Printf("Have received prepare before pre-prepare - appending new entry to logs")
						newEntry := logEntry{viewId: prepareMsg.ViewId, sequenceID: prepareMsg.SequenceID, pre: make([]*pb.PrepareMsg, msgLimit), com: make([]*pb.CommitMsg, msgLimit), prepared: false, committed: false, committedLocal: false}
						oldPrepares := newEntry.pre
						oldPrepares = append(oldPrepares, prepareMsg)
						newEntry.pre = oldPrepares
						logEntries = append(logEntries, newEntry)
					}
					if prepared {
						digest := prepareMsg.Digest
						if isByzantine {
							digest = tamper(digest)
						}
						commitMsg := pb.CommitMsg{ViewId: prepareMsg.ViewId, SequenceID: prepareMsg.SequenceID, Digest: prepareMsg.Digest, Node: strconv.FormatInt(nodeID+3001, 10)}
						for p, c := range peerClients {
							go func(c pb.PbftClient, p string) {
								_, _ = c.CommitPBFT(context.Background(), &commitMsg)
							}(c, p)
						}
						oldEntry := logEntries[prepareMsg.SequenceID]
						oldCommits := oldEntry.com
						oldCommits = append(oldCommits, &commitMsg)
						oldEntry.com = oldCommits
						logEntries[prepareMsg.SequenceID] = oldEntry
					}
				}
				printMyStoreAndLog(logEntries, s, currentView, curreSeqID)
			} else {
				log.Printf("Received PrepareMsgChan %v", prepareMsg)
				log.Printf("But.....Requested View Change")
				log.Printf("Send Back Redirect message - View Change")
			}
		case c := <-pbft.CommitMsgChan:
			if !viewChangePhase {
				commitMsg := c.Arg
				log.Printf("Received CommitMsgChan %v", commitMsg.Node)
				printCommitMsg(*commitMsg, currentView, curreSeqID)
				verified := verifyCommit(commitMsg, currentView, curreSeqID, logEntries)
				if verified {
					if vcTimer.TimeRemaining() < 100*time.Millisecond {
						dur := util.RandomDuration(r)
						log.Printf("Resetting timer for duration - %v", dur)
						vcTimer.Reset(dur)
					}
					oldEntry := logEntries[commitMsg.SequenceID]
					oldCommits := oldEntry.com
					oldCommits = append(oldCommits, commitMsg)
					oldEntry.com = oldCommits
					logEntries[commitMsg.SequenceID] = oldEntry
					committed := isCommitted(oldEntry, numberOfPeers)
					oldEntry.committed = committed
					committedLocal := isCommittedLocal(oldEntry, numberOfPeers)
					oldEntry.committedLocal = committedLocal
					logEntries[commitMsg.SequenceID] = oldEntry
					if committedLocal {
						vcTimer.Stop()
						// Execute and finally send back to client to aggregate
						clr := oldEntry.clientReq
						s.HandleCommand(clr, currentView, id, curreSeqID)
					}
				}
				printMyStoreAndLog(logEntries, s, currentView, curreSeqID)
			} else {
				log.Printf("Received CommitMsgChan %v", c.Arg.Node)
				log.Printf("But.....Requested View Change")
				log.Printf("Send Back Redirect message - View Change")
			}
		}
	}
	log.Printf("Strange to arrive here")
}
