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
	"bytes"
	"encoding/gob"
	"runtime"
)

// import "bytes"
// import "encoding/gob"

// **** notes
// *************************************************
// referenced https://github.com/doggeral/6.824-golab
// for the channel and time.After way of implementing
// election as well as heartbeat mechanism instead of
// using timer, though the latter is more intuitive,
// but may cause some interleaves for example sending
// AppendEntriesArgs
// *************************************************


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
)

// log entry
type LogEntry struct {
	Command     interface{}
	Term int
	Index int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// persistent state on all servers
	currentTerm int
	votedFor    int
	log []LogEntry

	commitIndex int
	lastApplied int

	nextIndex []int
	matchIndex []int

	applyCh chan ApplyMsg

	state int

	chanCommit chan bool
	chanGrantVote chan bool
	chanLeader chan bool
	chanHeart chan bool

	votesGranted int

	leaderId int
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	Term int
	VoteGranted bool
}

//
// example AppendEntries RPC arguments structure.
//
type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

//
// example AppendEntries RPC reply structure.
//
type AppendEntriesReply struct {
	Term int
	Success bool
	NextIndex int
}

type InstallSnapshotArgs struct {
	Term int
	LeaderId int
	LastIncludedIndex int
	LastIncludedTerm int
	Offset int
	Data[] byte
	Done bool
}

type InstallSnapshotReply struct {
	Term int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.currentTerm, rf.state == LEADER
}

func (rf *Raft) GetRaftLogSize() int {
	return rf.persister.RaftStateSize()
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := rf.currentTerm
	isLeader := rf.state == LEADER
	if isLeader {
		index = rf.lastIndex() + 1
		rf.log = append(rf.log, LogEntry{Command:command,Term:term,Index:index})

		rf.persist()
	}

	return index, term, isLeader
}

func (rf *Raft) DoSnapshot(data []byte, appliedIndex int){
	rf.mu.Lock()
	defer rf.mu.Unlock()

	baseIndex := rf.log[0].Index
	if appliedIndex <= baseIndex || appliedIndex > rf.lastIndex(){
		return
	}

	var newLog []LogEntry
	newLog = append(newLog, LogEntry{Index:appliedIndex,Term:rf.log[appliedIndex-baseIndex].Term})
	for i:=appliedIndex+1;i<=rf.lastIndex();i++{
		newLog = append(newLog,rf.log[i-baseIndex])
	}
	rf.log = newLog
	rf.persist()

	// persist log up to appliedIndex
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.log[0].Index)
	e.Encode(rf.log[0].Term)

	// do not e.Encode(snapshot), because snapshot already encoded
	snapshot := w.Bytes()
	snapshot = append(snapshot, data...)
	rf.persister.SaveSnapshot(snapshot)
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	 w := new(bytes.Buffer)
	 e := gob.NewEncoder(w)
	 e.Encode(rf.currentTerm)
	 e.Encode(rf.votedFor)
	 e.Encode(rf.log)
	 data := w.Bytes()
	 rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	 r := bytes.NewBuffer(data)
	 d := gob.NewDecoder(r)
	 d.Decode(&rf.currentTerm)
	 d.Decode(&rf.votedFor)
	 d.Decode(&rf.log)
}

func (rf *Raft) readSnapshot(data []byte) {
	if len(data) > 0 {
		r := bytes.NewBuffer(data)
		d := gob.NewDecoder(r)

		var index int
		var term int
		d.Decode(&index)
		d.Decode(&term)

		rf.commitIndex = index
		rf.lastApplied = index

		rf.log = rf.cutLog(index,term,rf.log)

		go func(){
			// force service to use snapshot data
			rf.applyCh <- ApplyMsg{UseSnapshot:true,Snapshot:data}
		}()
	}
}

func (rf *Raft) cutLog(lastIndex int, lastTerm int, log []LogEntry) []LogEntry{
	var newLog []LogEntry
	newLog = append(newLog, LogEntry{Index:lastIndex,Term:lastTerm})
	for i := range log{
		if log[i].Index == lastIndex && log[i].Term == lastTerm{
			newLog = append(newLog,log[i+1:]...)
			break
		}
	}

	return newLog
}

func (rf *Raft) lastIndex() int{
	return rf.log[len(rf.log)-1].Index
}

func (rf *Raft) lastTerm() int{
	return rf.log[len(rf.log)-1].Term
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

func (rf *Raft) toLeader(){
	rf.state = LEADER
	rf.leaderId = rf.me

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	index := rf.lastIndex() + 1
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = index
		rf.matchIndex[i] = rf.log[0].Index
	}
}

func (rf *Raft) toCandidate(){
	rf.state = CANDIDATE
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.votesGranted = 1
	rf.leaderId = -1

	rf.persist()
}

func (rf *Raft) toFollower(term, leaderId int){
	rf.state = FOLLOWER
	rf.currentTerm = term
	rf.votedFor = -1
	rf.leaderId = leaderId
	rf.votesGranted = 1

	rf.persist()
}

func (rf *Raft) sendInstallSnapshot(server int, args InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if ok {
		if reply.Term > rf.currentTerm{
			rf.toFollower(reply.Term, -1)
			return ok
		}

		rf.nextIndex[server] = args.LastIncludedIndex + 1
		rf.matchIndex[server] = args.LastIncludedIndex
	}

	return ok
}

func (rf *Raft) InstallSnapshot(args InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	rf.chanHeart <- true

	if args.Term > rf.currentTerm {
		rf.toFollower(args.Term, args.LeaderId)
	}
	reply.Term = rf.currentTerm

	rf.persister.SaveSnapshot(args.Data)

	rf.log = rf.cutLog(args.LastIncludedIndex,args.LastIncludedTerm,rf.log)
	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex
	rf.persist()

	rf.applyCh <- ApplyMsg{UseSnapshot:true,Snapshot:args.Data}

}

// start election
func (rf *Raft) voting() {
	req := RequestVoteArgs{}

	rf.mu.Lock()
	req.Term = rf.currentTerm
	req.CandidateId = rf.me
	req.LastLogIndex = rf.lastIndex()
	req.LastLogTerm = rf.lastTerm()
	rf.mu.Unlock()

	for i :=0;i<len(rf.peers);i++{
		if i != rf.me && rf.state == CANDIDATE{
			go func(i int){
				reply := RequestVoteReply{}
				rf.sendRequestVote(i, req, &reply)
			}(i)
		}
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

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if ok{
		if rf.state != CANDIDATE || args.Term != rf.currentTerm{
			return ok
		}

		if reply.Term > rf.currentTerm {
			rf.toFollower(reply.Term, -1)
		}

		if rf.state == CANDIDATE{
			if reply.VoteGranted{
				rf.votesGranted += 1
			}

			if rf.votesGranted > (len(rf.peers)/2){
				rf.chanLeader <- true
			}
		}
	}

	return ok
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

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
		/*
		Raft determines which of two logs is more up-to-date
		by comparing the index and term of the last entries in the
		logs. If the logs have last entries with different terms, then
		the log with the later term is more up-to-date. If the logs
		end with the same term, then whichever log is longer is
		more up-to-date.
		*/
		if args.LastLogTerm > rf.lastTerm() ||(args.LastLogTerm == rf.lastTerm() && args.LastLogIndex >= rf.lastIndex()){
			rf.chanGrantVote <- true
			rf.toFollower(args.Term, -1)
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
		}
	}
}

func (rf *Raft) heartbeats(){
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == LEADER {
		// If there exists an N such that N > commitIndex, a majority
		// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
		// set commitIndex = N (§5.3, §5.4).
		// leader only counts a majority of replicas for currentTerm
		N := rf.commitIndex
		last := rf.lastIndex()
		baseIndex := rf.log[0].Index
		for i := rf.commitIndex + 1; i <= last; i++ {
			num := 1
			for j := range rf.peers {
				if j != rf.me && rf.matchIndex[j] >= i && rf.log[i-baseIndex].Term == rf.currentTerm {
					num++
				}
			}
			if 2*num > len(rf.peers) {
				N = i
			}
		}
		if N != rf.commitIndex {
			rf.commitIndex = N
			rf.chanCommit <- true
		}
	}

	for i := range rf.peers {
		if i != rf.me && rf.state == LEADER{
			baseIndex := rf.log[0].Index
			if rf.nextIndex[i] <= baseIndex {
				// install snapshot
				var args InstallSnapshotArgs
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.Data = rf.persister.snapshot
				args.LastIncludedIndex = rf.log[0].Index
				args.LastIncludedTerm = rf.log[0].Term

				go func(i int){
					var reply InstallSnapshotReply
					rf.sendInstallSnapshot(i, args, &reply)
				}(i)
			}else{
				var args AppendEntriesArgs
				args.LeaderCommit = rf.commitIndex
				args.LeaderId = rf.me
				args.PrevLogIndex = rf.nextIndex[i]-1
				args.Term = rf.currentTerm
				args.PrevLogTerm = rf.log[args.PrevLogIndex-baseIndex].Term
				args.Entries = make([]LogEntry, len(rf.log[args.PrevLogIndex+1-baseIndex:]))
				copy(args.Entries, rf.log[args.PrevLogIndex+1-baseIndex:])

				go func(i int){
					var reply AppendEntriesReply
					rf.sendAppendEntries(i, args, &reply)
				}(i)
			}
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if ok {
		if rf.state != LEADER || args.Term != rf.currentTerm{
			return ok
		}

		if reply.Term > rf.currentTerm {
			rf.toFollower(reply.Term, -1)
			return ok
		}

		if reply.Success {
			if len(args.Entries) > 0{
				rf.nextIndex[server] = reply.NextIndex
				rf.matchIndex[server] = reply.NextIndex - 1
			}
		}else{
			// append entry failed does not mean reply.NextIndex invalid
			rf.nextIndex[server] = reply.NextIndex
		}
	}

	return ok
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Success = false
	reply.Term = rf.currentTerm
	reply.NextIndex = 0

	if args.Term < rf.currentTerm {
		reply.NextIndex = rf.lastIndex() + 1
		return
	}

	rf.chanHeart <- true

	if args.Term > rf.currentTerm {
		rf.toFollower(args.Term, args.LeaderId)
	}
	reply.Term = rf.currentTerm

	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	if args.PrevLogIndex > rf.lastIndex() {
		reply.NextIndex = rf.lastIndex() + 1
		return
	}
	baseIndex := rf.log[0].Index
	if args.PrevLogIndex > baseIndex && rf.log[args.PrevLogIndex-baseIndex].Term != args.PrevLogTerm{
		reply.NextIndex = args.PrevLogIndex
		for i := len(rf.log)-1;i>=0;i--{
			if rf.log[args.PrevLogIndex-baseIndex].Term == rf.log[i].Term{
				reply.NextIndex = rf.log[i].Index
			}else{
				break
			}
		}
		return
	}

	if args.PrevLogIndex >= baseIndex{
		tmp := rf.log[:args.PrevLogIndex+1-baseIndex]
		tmp = append(tmp, args.Entries...)
		rf.log = tmp
		rf.toFollower(args.Term, args.LeaderId)

		reply.Success = true
		reply.Term = rf.currentTerm
		reply.NextIndex = rf.lastIndex() + 1
	}else{
		// this can happen after rf snapshot, but leader has
		// not updated corresponding nextIndex entry
		reply.Term = rf.currentTerm
		reply.NextIndex = rf.log[0].Index + 1
	}

	// If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		last := rf.lastIndex()
		if args.LeaderCommit > last{
			rf.commitIndex = last
		}else{
			rf.commitIndex = args.LeaderCommit
		}

		rf.chanCommit <- true
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
	rf.leaderId = -1
	rf.votesGranted = 1
	rf.log = append(rf.log, LogEntry{Term:0,Index:0})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.chanCommit = make(chan bool,100)
	rf.chanGrantVote = make(chan bool,100)
	rf.chanLeader = make(chan bool,100)
	rf.chanHeart = make(chan bool,100)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())

	go rf.followerDaemon()
	go rf.candidateDaemon()
	go rf.leaderDaemon()
	go rf.commitDaemon()

	return rf
}

func (rf *Raft) followerDaemon(){
	for {
		if rf.state == FOLLOWER{
			select {
			case <- rf.chanHeart:
			case <- rf.chanGrantVote:
			case <- time.After(time.Duration(rand.Int63()%333+550)*time.Millisecond):
				rf.state = CANDIDATE
			}
		}else{
			runtime.Gosched()
		}
	}
}

func (rf *Raft) candidateDaemon(){
	for {
		if rf.state == CANDIDATE{
			rf.mu.Lock()
			rf.toCandidate()
			rf.mu.Unlock()
			rf.voting()
			select {
			case <- rf.chanHeart:
				rf.state = FOLLOWER
			case <- rf.chanLeader:
				rf.mu.Lock()
				rf.toLeader()
				rf.mu.Unlock()
			case <- time.After(time.Duration(rand.Int63()%333+550)*time.Millisecond):
			}
		}else{
			runtime.Gosched()
		}
	}
}

func (rf *Raft) leaderDaemon(){
	for {
		if rf.state == LEADER{
			rf.heartbeats()

			time.Sleep(50*time.Millisecond)
		}else{
			time.Sleep(10*time.Millisecond)
		}
	}
}

func (rf *Raft) commitDaemon(){
	for {
		select {
		case <- rf.chanCommit:
			rf.mu.Lock()
			// index out of range without i <= rf.lastIndex()
			baseIndex := rf.log[0].Index
			for i := rf.lastApplied+1;i<=rf.commitIndex && i <= rf.lastIndex();i++{
				rf.applyCh <- ApplyMsg{Index:i,Command:rf.log[i-baseIndex].Command}
				rf.lastApplied = i
			}
			rf.mu.Unlock()
		}
	}
}