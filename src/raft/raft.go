package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import (
	"labrpc"
	"time"
	"math/rand"
)

// import "bytes"
// import "encoding/gob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

const (
	FOLLOWER   = 1
	CANDIDATE  = 2
	LEADER     = 3
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state on all servers
	currentTerm int
	votedFor    int

	applyCh chan ApplyMsg

	ticker *time.Ticker
	heartBeatTicker *time.Ticker

	state int // follower, candidate, leader
	hadValidRpc bool // received msg during election timeout
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	term = rf.currentTerm
	isleader = rf.state == LEADER
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}




//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term int
	CandidateId int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId{
		rf.state = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		rf.hadValidRpc = true

		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.state = FOLLOWER
		rf.currentTerm = args.Term
		//rf.votedFor = -1
		rf.votedFor = args.CandidateId
		rf.hadValidRpc = true

		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		return
	}

	reply.VoteGranted = false
	reply.Term = rf.currentTerm
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}



//
// example AppendEntries RPC arguments structure.
//
type AppendEntriesArgs struct {
	// Your data here.
	Term int
	LeaderId int
}

//
// example AppendEntries RPC reply structure.
//
type AppendEntriesReply struct {
	// Your data here.
	Term int
	Success bool
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = true

		rf.state = FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.hadValidRpc = true
		return
	}


	if rf.state == FOLLOWER {
		reply.Term = rf.currentTerm
		reply.Success = true

		rf.hadValidRpc = true
		rf.votedFor = -1
	} else if rf.state == CANDIDATE {
		reply.Term = rf.currentTerm
		reply.Success = true

		rf.state = FOLLOWER
		rf.hadValidRpc = true
		rf.votedFor = -1
	}else{
		reply.Term = rf.currentTerm
		reply.Success = false
		DPrintf("heartbeat should not be here!\n")
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := rf.currentTerm
	isLeader := rf.state == LEADER

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.applyCh = applyCh
	rf.state = FOLLOWER
	rf.hadValidRpc = false
	rf.ticker = time.NewTicker(time.Duration(randInt(15,31)) * 10 * time.Millisecond)
	go func() {
		for {
			select {
			case <- rf.ticker.C:
			// follower without valid rpc
				rf.mu.Lock()

				if rf.state == FOLLOWER && rf.hadValidRpc {
					rf.hadValidRpc = false
				} else if rf.state == FOLLOWER && !rf.hadValidRpc {
					rf.state = CANDIDATE
					rf.currentTerm += 1
					rf.votedFor = rf.me

					req := RequestVoteArgs{}
					req.Term = rf.currentTerm
					req.CandidateId = rf.me

					votes := 1
					for i :=0;i<len(rf.peers);i++{
						if i != rf.me {
							rep := RequestVoteReply{}
							ok := rf.sendRequestVote(i, req, &rep)
							if !ok {
								DPrintf("%d sendRequestVote %d failed\n",rf.me,i)
							}

							if rep.Term > rf.currentTerm {
								// roll back to follower
								rf.state = FOLLOWER
								rf.currentTerm = rep.Term
								rf.votedFor = -1
								rf.hadValidRpc = false
								votes = 0
								break;
							}

							if rep.VoteGranted {
								votes += 1
							}
						}
					}

					if votes > (len(rf.peers)/2){
						rf.state = LEADER
						rf.hadValidRpc = false
						// send heartbeat
						req := AppendEntriesArgs{}
						req.Term = rf.currentTerm
						req.LeaderId = rf.me

						for i :=0;i<len(rf.peers);i++{
							if i != rf.me {
								rep := AppendEntriesReply{}
								ok := rf.sendAppendEntries(i, req, &rep)
								if !ok {
									DPrintf("%d sendAppendEntries %d failed\n",rf.me,i)
								}

								if rep.Term > rf.currentTerm {
									rf.state = FOLLOWER
									rf.currentTerm = rep.Term
									rf.votedFor = -1
									rf.hadValidRpc = false
									break;
								}
							}
						}
					}
				} else if rf.state == CANDIDATE{
					rf.currentTerm += 1
					rf.votedFor = rf.me
					rf.hadValidRpc = false

					req := RequestVoteArgs{}
					req.Term = rf.currentTerm
					req.CandidateId = rf.me

					votes := 1
					for i :=0;i<len(rf.peers);i++{
						if i != rf.me {
							rep := RequestVoteReply{}
							ok := rf.sendRequestVote(i, req, &rep)
							if !ok {
								DPrintf("%d sendRequestVote %d failed\n",rf.me,i)
							}

							if rep.Term > rf.currentTerm {
								// roll back to follower
								rf.state = FOLLOWER
								rf.currentTerm = rep.Term
								rf.votedFor = -1
								rf.hadValidRpc = false
								votes = 0
								break;
							}

							if rep.VoteGranted {
								votes += 1
							}
						}
					}

					if votes > (len(rf.peers)/2){
						rf.state = LEADER
						rf.hadValidRpc = false
						// send heartbeat
						req := AppendEntriesArgs{}
						req.Term = rf.currentTerm
						req.LeaderId = rf.me

						for i :=0;i<len(rf.peers);i++{
							if i != rf.me {
								rep := AppendEntriesReply{}
								ok := rf.sendAppendEntries(i, req, &rep)
								if !ok {
									DPrintf("%d sendAppendEntries %d failed\n",rf.me,i)
								}

								if rep.Term > rf.currentTerm {
									rf.state = FOLLOWER
									rf.currentTerm = rep.Term
									rf.votedFor = -1
									rf.hadValidRpc = false
									break;
								}
							}
						}
					}
				} else if rf.state == LEADER {
					rf.hadValidRpc = false

					// send heartbeat
					req := AppendEntriesArgs{}
					req.Term = rf.currentTerm
					req.LeaderId = rf.me

					for i :=0;i<len(rf.peers);i++{
						if i != rf.me {
							rep := AppendEntriesReply{}
							ok := rf.sendAppendEntries(i, req, &rep)
							if !ok {
								DPrintf("%d sendAppendEntries %d failed\n",rf.me,i)
							}

							if rep.Term > rf.currentTerm {
								rf.state = FOLLOWER
								rf.currentTerm = rep.Term
								rf.votedFor = -1
								rf.hadValidRpc = false
								break;
							}
						}
					}
				} else{
					DPrintf("unknown state %d for server %d\n",rf.state, rf.me)
				}

				rf.mu.Unlock()
			}
		}
	}()


	rf.heartBeatTicker = time.NewTicker(time.Duration(randInt(15,31)) * time.Millisecond)
	go func() {
		for {
			select {
			case <- rf.heartBeatTicker.C:
				rf.mu.Lock()

				if rf.state == LEADER {
					req := AppendEntriesArgs{}
					req.Term = rf.currentTerm
					req.LeaderId = rf.me

					for i :=0;i<len(rf.peers);i++{
						if i != rf.me {
							rep := AppendEntriesReply{}
							ok := rf.sendAppendEntries(i, req, &rep)
							if !ok {
								DPrintf("%d sendAppendEntries %d failed\n",rf.me,i)
							}

							if rep.Term > rf.currentTerm {
								rf.state = FOLLOWER
								rf.currentTerm = rep.Term
								rf.votedFor = -1
								rf.hadValidRpc = false
								break;
							}
						}
					}
				}

				rf.mu.Unlock()
			}
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}

func randInt(min int, max int) int {
	return min + rand.Intn(max-min)
}
