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

type logEntry struct {
	com            []*pb.CommitMsg
	pre            []*pb.PrepareMsg
	viewId         int64
	prePrep        *pb.PrePrepareMsg
	prepared       bool
	clientReq      *pb.ClientRequest
	committed      bool
	sequenceID     int64
	committedLocal bool
}

func reqValidPrepare(n int64) int64 {
	f := (n - 1) / 3
	log.Printf("%v --------------------------------", f)
	return 2 * f
}

func reqValidCommit(n int64) int64 {
	f := (n - 1) / 3
	log.Printf("%v --------------------------------", f)
	return 1 + f
}

func reqValidCommitLocal(n int64) int64 {
	f := (n - 1) / 3
	log.Printf("%v --------------------------------", f)
	return 2*f + 1
}

func reqValidVC(n int64) int64 {
	f := (n - 1) / 3
	log.Printf("%v --------------------------------", f)
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
	if commitMsg.SequenceID+1 > int64(len(logEntries)) {
		return false
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
		log.Printf("Is prepared check number of valid prepares is %v, len of entry:pre is %v", validPrepares, entry.pre)
		// return validPrepares >= 2
	}
	return false
}

func isCommitted(entry logEntry, n int64) bool {
	log.Printf("Entering commiitted")
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
		// return validCommits >= reqValidCommit(n)
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

type boolgen struct {
	src       rand.Source
	cache     int64
	remaining int
}

func (b *boolgen) Bool() bool {
	if b.remaining == 0 {
		b.cache, b.remaining = b.src.Int63(), 63
	}

	result := b.cache&0x01 == 1
	b.cache >>= 1
	b.remaining--

	return result
}
func New() *boolgen {
	return &boolgen{src: rand.NewSource(time.Now().UnixNano())}
}

func tamper(digest string) string {
	return RandStringRunes(int64(len(digest)))
}

type ClientResponse struct {
	ret  *pb.ClientResponse
	err  error
	node string
}

func serve(s *KVStore, r *rand.Rand, peers *util.ArrayPeers, id string, port int, isByzantine bool) {
	faulty := New()
	var evluated map[int64]bool
	evluated = make(map[int64]bool)
	log.Printf("The byzantine condition is %v", isByzantine)
	pbft := util.Pbft{PbftMsgChan: make(chan util.PbftMsgInput, 20)}
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

	currentView := int64(0)
	curreSeqID := int64(-1)
	var logEntries []logEntry
	msgLimit := 0
	nodeID := int64(port) % 3001
	timer := time.NewTimer(util.RandomDuration(r))
	vcTimer := util.NewSecondsTimer(util.RandomDuration2(r))
	vcTimer.Stop()

	viewChangePhase := false
	numberOfVcr := int64(0)

	for {
		select {
		case <-timer.C:
			// printMyStoreAndLog(logEntries, s, currentView, curreSeqID)
			if curreSeqID != -1 && (curreSeqID+1 <= int64(len(logEntries))) {
				prevEntry := logEntries[curreSeqID]
				if !(prevEntry.committedLocal) {
					if isCommittedLocal(prevEntry, numberOfPeers) {
						log.Printf("Deadlock case check")
						prevEntry.committedLocal = true
						clr := prevEntry.clientReq
						s.HandleCommand(clr, currentView, id, curreSeqID)
						evluated[clr.Timestamp] = true
					}
				}
			}
			util.RestartTimer(timer, r)

		case <-vcTimer.Timer.C:
			if vcTimer.TimeRemaining() < 100*time.Millisecond {
				dur := util.RandomDuration(r)
				log.Printf("Resetting timer for duration - %v", dur)
				vcTimer.Reset(dur)
			}
			newView := (currentView + 1) % numberOfPeers
			currentView = newView
			log.Printf("Timeout - initiate view change. New view - %v", newView)
			viewChangePhase = true
			sequID := curreSeqID
			if curreSeqID > 0 {
				sequID -= 1
			}
			viewChange_temp := pb.ViewChangeMsg{Type: "view-change", NewView: newView, LastSequenceID: sequID, Node: id}
			viewChange := pb.Msg_Vcm{Vcm: &viewChange_temp}
			for p, c := range peerClients {
				go func(c pb.PbftClient, p string) {
					//time.Sleep(100 * time.Millisecond)
					_, _ = c.SendPbftMsg(context.Background(), &pb.Msg{Operation: "ViewChange", Arg: &viewChange})
				}(c, p)
			}

		case inputChan := <-pbft.PbftMsgChan:
			msg := inputChan.Arg
			switch opt := msg.GetOperation(); opt {

			case "Redirect":
				prePreMsg := msg.GetPpm()
				cr := prePreMsg.Request
				val, exists := evluated[cr.Timestamp]
				_ = faulty.Bool
				// checkfaulty := faulty.Bool()
				checkfaulty := true
				if !isByzantine {
					checkfaulty = true
				}
				if checkfaulty {
					if exists && val {
						s.HandleCommand(cr, currentView, id, curreSeqID)
					} else {
						evluated[cr.Timestamp] = false
						if !viewChangePhase {
							if currentView == nodeID {
								log.Printf("Received client request in view %v and seq %v from %v", currentView, curreSeqID, cr.ClientID)
								curreSeqID += 1
								digest := util.Digest(cr)
								if isByzantine {
									digest = tamper(digest)
								}
								prePreMsg := pb.PrePrepareMsg{ViewId: currentView, SequenceID: curreSeqID, Digest: digest, Request: cr, Node: id}
								prePreMsg_temp := pb.Msg_Ppm{Ppm: &prePreMsg}
								for p, c := range peerClients {
									go func(c pb.PbftClient, p string) {
										_, _ = c.SendPbftMsg(context.Background(), &pb.Msg{Operation: "PrePrepare", Arg: &prePreMsg_temp})

									}(c, p)
									log.Printf("Sending PrePrepare to %v for current view %v, sequenceID %v", p, currentView, curreSeqID)
								}
								newEntry := logEntry{viewId: currentView, sequenceID: curreSeqID, clientReq: cr, prePrep: &prePreMsg,
									pre: make([]*pb.PrepareMsg, msgLimit), com: make([]*pb.CommitMsg, msgLimit), prepared: false,
									committed: false, committedLocal: false}
								if int64(len(logEntries)) >= curreSeqID+1 {
									logEntries[curreSeqID] = newEntry
								} else {
									logEntries = append(logEntries, newEntry)
								}

							} else {
								if vcTimer.TimeRemaining() < 100*time.Millisecond {
									dur := util.RandomDuration2(r)
									log.Printf("Resetting timer for duration - %v", dur)
									vcTimer.Reset(dur)
								}
								creq := logEntries[len(logEntries)-1].clientReq
								ppm := pb.PrePrepareMsg{ViewId: currentView, SequenceID: curreSeqID, Digest: util.Digest(creq), Request: creq, Node: id}
								ppm_temp := pb.Msg_Ppm{Ppm: &ppm}
								pp := "127.0.0.1:" + strconv.FormatInt(currentView+3001, 10)
								cc := peerClients[pp]
								go func(c pb.PbftClient, p string) {
									//time.Sleep(100 * time.Millisecond)
									_, _ = c.SendPbftMsg(context.Background(), &pb.Msg{Operation: "Redirect", Arg: &ppm_temp})

								}(cc, pp)

								// result := pb.Result{Result: &pb.Result_Redirect{Redirect: &pb.Redirect{Server: strconv.FormatInt(currentView+3005, 10)}}}
								// clientID := cr.ClientID
								// client, err := util.ConnectToClient(clientID) //client connection
								// if err != nil {
								// 	log.Fatalf("Failed to connect to GRPC server %v", err)
								// }
								// log.Printf("Connected to %v", clientID)
								// go func(c pb.PbftClient) {
								// 	crp_temp := pb.ClientResponse{ViewId: int64(0), Timestamp: int64(0), ClientID: "", Node: "", NodeResult: &result, SequenceID: int64(-1)}
								// 	crp := pb.Msg_Crm{Crm: &crp_temp}
								// 	c.SendPbftMsg(context.Background(),
								// 		&pb.Msg{Operation: "Redirect", Arg: &crp})
								// }(client)
								log.Printf("Send Back Redirect message - Wrong primary, connected back to primary %v", pp)
							}
						} else {
							log.Printf("Received ClientRequestChan %v", cr.ClientID)
							log.Printf("But.....Requested View Change")
						}
					}
				}

			case "ViewChange":
				vc := msg.GetVcm()
				newView := vc.NewView
				if vc.Type == "new-view" {
					vcTimer.Stop()
					log.Printf("Switching to new view - %v || %v", newView, vc)
					// Should send back redirect here for commands that weren't committed
					checkPrimary := currentView == nodeID
					currentView = newView
					viewChangePhase = false
					numberOfVcr = int64(0)
					curreSeqID = vc.LastSequenceID

					if checkPrimary {
						creq := logEntries[len(logEntries)-1].clientReq
						ppm := pb.PrePrepareMsg{ViewId: currentView, SequenceID: curreSeqID, Digest: util.Digest(creq), Request: creq, Node: id}
						ppm_temp := pb.Msg_Ppm{Ppm: &ppm}
						pp := "127.0.0.1:" + strconv.FormatInt(newView+3001, 10)
						cc := peerClients[pp]
						go func(c pb.PbftClient, p string) {
							//time.Sleep(100 * time.Millisecond)
							_, _ = c.SendPbftMsg(context.Background(), &pb.Msg{Operation: "Redirect", Arg: &ppm_temp})

						}(cc, pp)

						// result := pb.Result{Result: &pb.Result_Redirect{Redirect: &pb.Redirect{Server: strconv.FormatInt(newView+3005, 10)}}}
						// clientID := logEntries[len(logEntries)-1].clientReq.ClientID
						// client, err := util.ConnectToClient(clientID) //client connection
						// if err != nil {
						// 	log.Fatalf("Failed to connect to GRPC server %v", err)
						// }
						// log.Printf("Connected to %v", clientID)
						// go func(c pb.PbftClient) {
						// 	crp_temp := pb.ClientResponse{ViewId: int64(0), Timestamp: int64(0), ClientID: "", Node: "", NodeResult: &result, SequenceID: int64(-1)}
						// 	crp := pb.Msg_Crm{Crm: &crp_temp}
						// 	//time.Sleep(100 * time.Millisecond)
						// 	c.SendPbftMsg(context.Background(),
						// 		&pb.Msg{Operation: "Redirect", Arg: &crp})
						// }(client)
						log.Printf("Send Back Redirect message - View Change")
					}
				} else {
					if newView == nodeID {
						log.Printf("Received vote from %v", vc.Node)
						numberOfVcr += 1
					} else {
						log.Printf("Received view change request - %v", vc)
					}
				}
				// New Primary
				if numberOfVcr >= reqValidVC(numberOfPeers) {
					vcTimer.Stop()
					log.Printf("Switching to new view - %v and taking on as primary", newView)
					viewChange_temp := pb.ViewChangeMsg{Type: "new-view", NewView: newView, LastSequenceID: curreSeqID - 1, Node: strconv.FormatInt(newView+3001, 10)}
					viewChange := pb.Msg_Vcm{Vcm: &viewChange_temp}
					for p, c := range peerClients {
						go func(c pb.PbftClient, p string) {
							//time.Sleep(100 * time.Millisecond)
							_, _ = c.SendPbftMsg(context.Background(), &pb.Msg{Operation: "ViewChange", Arg: &viewChange})
						}(c, p)
						log.Printf("Sending view change to %v", p)
					}
					viewChangePhase = false
					currentView = newView
					numberOfVcr = 0
					curreSeqID = vc.LastSequenceID
				}
			case "PrePrepare":
				prePreMsg := msg.GetPpm()
				curreSeqID = prePreMsg.SequenceID
				if !viewChangePhase {
					if vcTimer.TimeRemaining() < 100*time.Millisecond {
						dur := util.RandomDuration2(r)
						log.Printf("Resetting timer for duration - %v", dur)
						vcTimer.Reset(dur)
					}
					log.Printf("Received pre-prepare request %v in view %v and seq %v from %v", prePreMsg, currentView, curreSeqID, prePreMsg.Node)
					verified := verifyPrePrepare(prePreMsg, currentView, curreSeqID, logEntries)
					if verified {
						digest := prePreMsg.Digest
						if isByzantine {
							digest = tamper(digest)
						}
						prepareMsg := pb.PrepareMsg{ViewId: prePreMsg.ViewId, SequenceID: prePreMsg.SequenceID, Digest: digest, Node: strconv.FormatInt(nodeID+3001, 10)}
						if prePreMsg.SequenceID+1 <= int64(len(logEntries)) {
							log.Printf("Had received a prepare msg before, so writing on previous curreSeqID - %v", prePreMsg.SequenceID)
							prevEntry := logEntries[prePreMsg.SequenceID]
							prevEntry.prePrep = prePreMsg
							prevEntry.clientReq = prePreMsg.Request
							prevPrepares := prevEntry.pre
							prevPrepares = append(prevPrepares, &prepareMsg)
							prevEntry.pre = prevPrepares
							logEntries[prePreMsg.SequenceID] = prevEntry
						} else {
							log.Printf("Appending new entry to log")
							newEntry := logEntry{viewId: prePreMsg.ViewId, sequenceID: prePreMsg.SequenceID, clientReq: prePreMsg.Request, prePrep: prePreMsg, pre: make([]*pb.PrepareMsg, msgLimit), com: make([]*pb.CommitMsg, msgLimit), prepared: false, committed: false, committedLocal: false}
							prevPrepares := newEntry.pre
							prevPrepares = append(prevPrepares, &prepareMsg)
							newEntry.pre = prevPrepares
							logEntries = append(logEntries, newEntry)

							//////fancy////////////////////////
							prepared := false
							prevEntryC := logEntries[len(logEntries)-1]
							if !(prevEntryC.prepared) {
								prepared = isPrepared(prevEntryC, numberOfPeers)
								prevEntryC.prepared = prepared
							}
							if prepared {
								log.Printf("It is already prepared")
								digest := prepareMsg.Digest
								if isByzantine {
									digest = tamper(digest)
								}
								commitMsg := pb.CommitMsg{ViewId: prePreMsg.ViewId, SequenceID: prePreMsg.SequenceID, Digest: prePreMsg.Digest, Node: strconv.FormatInt(nodeID+3001, 10)}
								commitMsg_temp := pb.Msg_Cm{Cm: &commitMsg}
								for p, c := range peerClients {
									go func(c pb.PbftClient, p string) {
										//time.Sleep(100 * time.Millisecond)
										_, _ = c.SendPbftMsg(context.Background(), &pb.Msg{Operation: "Commit", Arg: &commitMsg_temp})

									}(c, p)
									log.Printf("Sending Commit to %v for current view %v, sequenceID %v", p, currentView, curreSeqID)
								}
								prevEntry := logEntries[prePreMsg.SequenceID]
								prevCommits := prevEntry.com
								prevCommits = append(prevCommits, &commitMsg)
								prevEntry.com = prevCommits
								logEntries[prePreMsg.SequenceID] = prevEntry
							}
						}
						prepareMsg_temp := pb.Msg_Pm{Pm: &prepareMsg}
						for p, c := range peerClients {
							go func(c pb.PbftClient, p string) {
								//time.Sleep(100 * time.Millisecond)
								_, _ = c.SendPbftMsg(context.Background(), &pb.Msg{Operation: "Prepare", Arg: &prepareMsg_temp})

							}(c, p)
							log.Printf("Sending Prepare to %v for current view %v, sequenceID %v", p, currentView, curreSeqID)
						}
					}

				} else {
					log.Printf("Received PrePrepareMsgChan %v from primary %v", prePreMsg, prePreMsg.Node)
					log.Printf("But.....Requested View Change")
					log.Printf("Send Back Redirect message - View Change")
				}
			case "Prepare":
				prepareMsg := msg.GetPm()
				if !viewChangePhase {
					log.Printf("Received prepare request %v in view %v and seq %v", prepareMsg, currentView, curreSeqID)
					verified := verifyPrepare(prepareMsg, currentView, curreSeqID, logEntries)
					if verified {
						if vcTimer.TimeRemaining() < 100*time.Millisecond {
							dur := util.RandomDuration2(r)
							log.Printf("Resetting timer for duration - %v", dur)
							vcTimer.Reset(dur)
						}
						prepared := false
						if prepareMsg.SequenceID+1 <= int64(len(logEntries)) {
							log.Printf("Normal case received pre-prepare before prepare - writing to entry in logs at - %v", prepareMsg.SequenceID)
							prevEntry := logEntries[prepareMsg.SequenceID]
							prevPrepares := prevEntry.pre
							prevPrepares = append(prevPrepares, prepareMsg)
							prevEntry.pre = prevPrepares
							logEntries[prepareMsg.SequenceID] = prevEntry
							if !(prevEntry.prepared) {
								prepared = isPrepared(prevEntry, numberOfPeers)
								prevEntry.prepared = prepared
							}
							logEntries[prepareMsg.SequenceID] = prevEntry
						} else {
							log.Printf("Have received prepare before pre-prepare - appending new entry to logs")
							newEntry := logEntry{viewId: prepareMsg.ViewId, sequenceID: prepareMsg.SequenceID, pre: make([]*pb.PrepareMsg, msgLimit), com: make([]*pb.CommitMsg, msgLimit), prepared: false, committed: false, committedLocal: false}
							prevPrepares := newEntry.pre
							prevPrepares = append(prevPrepares, prepareMsg)
							newEntry.pre = prevPrepares
							logEntries = append(logEntries, newEntry)
						}
						if prepared {
							log.Printf("It is already prepared")
							digest := prepareMsg.Digest
							if isByzantine {
								digest = tamper(digest)
							}
							commitMsg := pb.CommitMsg{ViewId: prepareMsg.ViewId, SequenceID: prepareMsg.SequenceID, Digest: prepareMsg.Digest, Node: strconv.FormatInt(nodeID+3001, 10)}
							commitMsg_temp := pb.Msg_Cm{Cm: &commitMsg}
							for p, c := range peerClients {
								go func(c pb.PbftClient, p string) {
									//time.Sleep(100 * time.Millisecond)
									_, _ = c.SendPbftMsg(context.Background(), &pb.Msg{Operation: "Commit", Arg: &commitMsg_temp})

								}(c, p)
								log.Printf("Sending Commit to %v for current view %v, sequenceID %v", p, currentView, curreSeqID)
							}
							prevEntry := logEntries[prepareMsg.SequenceID]
							prevCommits := prevEntry.com
							prevCommits = append(prevCommits, &commitMsg)
							prevEntry.com = prevCommits
							logEntries[prepareMsg.SequenceID] = prevEntry
						}
					}
				} else {
					log.Printf("Received PrepareMsgChan %v", prepareMsg)
					log.Printf("But.....Requested View Change")
					log.Printf("Send Back Redirect message - View Change")
				}
			case "Commit":
				commitMsg := msg.GetCm()
				if !viewChangePhase {
					log.Printf("Received commit request from %v in view %v and seq %v", commitMsg.Node, currentView, curreSeqID)
					verified := verifyCommit(commitMsg, currentView, curreSeqID, logEntries)
					if verified {
						if vcTimer.TimeRemaining() < 100*time.Millisecond {
							dur := util.RandomDuration2(r)
							log.Printf("Resetting timer for duration - %v", dur)
							vcTimer.Reset(dur)
						}
						committedLocal := false
						if commitMsg.SequenceID+1 <= int64(len(logEntries)) {
							log.Printf("Normal case received prepare before commit - writing to entry in logs at - %v", commitMsg.SequenceID)
							prevEntry := logEntries[commitMsg.SequenceID]
							prevCommits := prevEntry.com
							prevCommits = append(prevCommits, commitMsg)
							prevEntry.com = prevCommits
							logEntries[commitMsg.SequenceID] = prevEntry
							committed := isCommitted(prevEntry, numberOfPeers)
							prevEntry.committed = committed
							if !(prevEntry.committedLocal) {
								committedLocal = isCommittedLocal(prevEntry, numberOfPeers)
								prevEntry.committedLocal = committedLocal
							} else {
								vcTimer.Stop()
							}
							logEntries[commitMsg.SequenceID] = prevEntry
							if committedLocal {
								vcTimer.Stop()
								// Execute and finally send back to client to aggregate
								clr := prevEntry.clientReq
								//time.Sleep(100 * time.Millisecond)
								s.HandleCommand(clr, currentView, id, curreSeqID)
								evluated[clr.Timestamp] = true

							}
						} else {
							log.Printf("Have received commit before prepare - appending new entry to logs")
							newEntry := logEntry{viewId: commitMsg.ViewId, sequenceID: commitMsg.SequenceID, pre: make([]*pb.PrepareMsg, msgLimit), com: make([]*pb.CommitMsg, msgLimit), prepared: false, committed: false, committedLocal: false}
							prevCommits := newEntry.com
							prevCommits = append(prevCommits, commitMsg)
							newEntry.com = prevCommits
							logEntries = append(logEntries, newEntry)
						}

					}

				} else {
					log.Printf("Received CommitMsgChan %v", commitMsg.Node)
					log.Printf("But.....Requested View Change")
					log.Printf("Send Back Redirect message - View Change")
				}
			default:
				log.Printf("Strange to arrive here1")
			}

		case inpChannel := <-s.C:
			cr := inpChannel.clientRequest
			val, exists := evluated[cr.Timestamp]
			_ = faulty.Bool
			// checkfaulty := faulty.Bool()
			checkfaulty := true
			if !isByzantine {
				checkfaulty = true
			}
			if checkfaulty {
				if exists && val {
					s.HandleCommand(cr, currentView, id, curreSeqID)
				} else {
					evluated[cr.Timestamp] = false
					if !viewChangePhase {
						if currentView == nodeID {
							log.Printf("Received client request in view %v and seq %v from %v", currentView, curreSeqID, cr.ClientID)
							curreSeqID += 1
							digest := util.Digest(cr)
							if isByzantine {
								digest = tamper(digest)
							}
							prePreMsg := pb.PrePrepareMsg{ViewId: currentView, SequenceID: curreSeqID, Digest: digest, Request: cr, Node: id}
							prePreMsg_temp := pb.Msg_Ppm{Ppm: &prePreMsg}
							for p, c := range peerClients {
								go func(c pb.PbftClient, p string) {
									_, _ = c.SendPbftMsg(context.Background(), &pb.Msg{Operation: "PrePrepare", Arg: &prePreMsg_temp})

								}(c, p)
								log.Printf("Sending PrePrepare to %v for current view %v, sequenceID %v", p, currentView, curreSeqID)
							}
							newEntry := logEntry{viewId: currentView, sequenceID: curreSeqID, clientReq: cr, prePrep: &prePreMsg,
								pre: make([]*pb.PrepareMsg, msgLimit), com: make([]*pb.CommitMsg, msgLimit), prepared: false,
								committed: false, committedLocal: false}
							if int64(len(logEntries)) >= curreSeqID+1 {
								logEntries[curreSeqID] = newEntry
							} else {
								logEntries = append(logEntries, newEntry)
							}

						} else {
							if vcTimer.TimeRemaining() < 100*time.Millisecond {
								dur := util.RandomDuration2(r)
								log.Printf("Resetting timer for duration - %v", dur)
								vcTimer.Reset(dur)
							}
							creq := cr
							ppm := pb.PrePrepareMsg{ViewId: currentView, SequenceID: curreSeqID, Digest: util.Digest(creq), Request: creq, Node: id}
							ppm_temp := pb.Msg_Ppm{Ppm: &ppm}
							pp := "127.0.0.1:" + strconv.FormatInt(currentView+3001, 10)
							cc := peerClients[pp]
							go func(c pb.PbftClient, p string) {
								//time.Sleep(100 * time.Millisecond)
								_, _ = c.SendPbftMsg(context.Background(), &pb.Msg{Operation: "Redirect", Arg: &ppm_temp})

							}(cc, pp)

							// result := pb.Result{Result: &pb.Result_Redirect{Redirect: &pb.Redirect{Server: strconv.FormatInt(currentView+3005, 10)}}}
							// clientID := cr.ClientID
							// client, err := util.ConnectToClient(clientID) //client connection
							// if err != nil {
							// 	log.Fatalf("Failed to connect to GRPC server %v", err)
							// }
							// log.Printf("Connected to %v", clientID)
							// go func(c pb.PbftClient) {
							// 	crp_temp := pb.ClientResponse{ViewId: int64(0), Timestamp: int64(0), ClientID: "", Node: "", NodeResult: &result, SequenceID: int64(-1)}
							// 	crp := pb.Msg_Crm{Crm: &crp_temp}
							// 	c.SendPbftMsg(context.Background(),
							// 		&pb.Msg{Operation: "Redirect", Arg: &crp})
							// }(client)
							log.Printf("Send Back Redirect message - Wrong primary, connected back to primary %v", pp)
						}
					} else {
						log.Printf("Received ClientRequestChan %v", cr.ClientID)
						log.Printf("But.....Requested View Change")
					}
				}
			}

		}
	}
	log.Printf("Strange to arrive here2")
}
