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
	_ = iota

	FOLLOWER
	CANDIDATE
	LEADER

	TM_HB = 30	// heartbeat timeout
	TM_EC = 150	// election timeout 150~300
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

	state int // follower, candidate, leader
	hadValidRpc bool // received valid rpc during election timeout

	electTimer *time.Timer
	hbTimer *time.Timer
	rvChan chan RequestVoteReply
	aeChan chan AppendEntriesReply

	votesGranted int

	leaderId int
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

	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.toFollower(args.Term, -1)
	}
	reply.Term = rf.currentTerm

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId{
		rf.toFollower(args.Term, -1)
		rf.votedFor = args.CandidateId
		rf.hadValidRpc = true

		reply.VoteGranted = true
	}
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

	reply.Success = false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	reply.Success = true
	rf.toFollower(args.Term, args.LeaderId)
	rf.hadValidRpc = true
	reply.Term = rf.currentTerm
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

func randDuration() time.Duration {
	return time.Duration(rand.Int63() % TM_EC + TM_EC) * time.Millisecond
}

func (rf *Raft) toLeader(){
	rf.state = LEADER
	rf.hadValidRpc = false
	rf.leaderId = rf.me
}

func (rf *Raft) toCandidate(){
	rf.state = CANDIDATE
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.hadValidRpc = false
	rf.votesGranted = 1
	rf.leaderId = -1
}

func (rf *Raft) toFollower(term, leaderId int){
	rf.state = FOLLOWER
	rf.currentTerm = term
	rf.votedFor = -1
	rf.hadValidRpc = false
	rf.leaderId = leaderId
	rf.votesGranted = 1
}

func (rf *Raft) heartbeats(){
	req := AppendEntriesArgs{}
	req.Term = rf.currentTerm
	req.LeaderId = rf.me

	for i :=0;i<len(rf.peers);i++{
		if i != rf.me {
			go func(i int) {
				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(i, req, &reply)
				if !ok {
					DPrintf("%d sendAppendEntries to %d failed\n",rf.me,i)
				} else{
					rf.aeChan <- reply
				}
			}(i)
		}
	}
}

func (rf *Raft) voting() {
	req := RequestVoteArgs{}
	req.Term = rf.currentTerm
	req.CandidateId = rf.me

	for i :=0;i<len(rf.peers);i++{
		if i != rf.me {
			go func(i int){
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(i, req, &reply)
				if !ok {
					DPrintf("%d sendRequestVote to %d failed\n",rf.me,i)
				}else{
					rf.rvChan <- reply
				}
			}(i)
		}
	}
}

func (rf *Raft) loop(){

	rf.hbTimer = time.NewTimer(TM_HB*time.Millisecond)
	rf.electTimer = time.NewTimer(randDuration())

	for {
		select {
		case reply := <- rf.aeChan:
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.toFollower(reply.Term, -1)
			}
			rf.mu.Unlock()
		case reply := <- rf.rvChan:
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.toFollower(reply.Term, -1)
			} else{
				if rf.state == CANDIDATE{
					if reply.VoteGranted{
						rf.votesGranted += 1
					}

					if rf.votesGranted > (len(rf.peers)/2){
						rf.toLeader()
						rf.heartbeats()
					}
				}
			}
			rf.mu.Unlock()
		case <- rf.hbTimer.C:
			rf.mu.Lock()
			rf.hbTimer.Reset(TM_HB*time.Millisecond)
			rf.mu.Unlock()

			if rf.state == LEADER {
				rf.heartbeats()
			}
		case <- rf.electTimer.C:
			rf.mu.Lock()

			rf.electTimer.Reset(randDuration())

			if rf.state == FOLLOWER && rf.hadValidRpc {
				rf.hadValidRpc = false
			} else if (rf.state == FOLLOWER && !rf.hadValidRpc) || rf.state == CANDIDATE {
				rf.toCandidate()
				rf.voting()
			} else if rf.state == LEADER {
			} else{
				DPrintf("unknown state %d for server %d\n",rf.state, rf.me)
			}

			rf.mu.Unlock()
		}
	}
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
	rf.leaderId = -1
	rf.votesGranted = 1
	rf.rvChan = make(chan RequestVoteReply, 100)
	rf.aeChan = make(chan AppendEntriesReply, 100)

	go rf.loop()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

