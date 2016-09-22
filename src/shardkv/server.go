package shardkv


// import "shardmaster"
import "labrpc"
import "raft"
import "sync"
import (
	"encoding/gob"
	"log"
	"time"
	"fmt"
	"bytes"
	"shardmaster"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key string
	Value string
	Cid int64
	Seq int64
	Action string
	Gid int
	Shard int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvs map[string]string // k-v data store
	oldRequests map[int64]int64 // preserve client request
	cNotifyChan map[int]chan Op // client notify channel
	sm       *shardmaster.Clerk
	config   shardmaster.Config
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	reply.WrongLeader = true
	reply.Err = ""

	if args.Gid != kv.gid{
		reply.Err = ErrWrongGroup
		return
	}

	op := Op{Key:args.Key,Value:"",Cid:args.Cid,Seq:args.Seq,Action:"Get",Gid:args.Gid,Shard:args.Shard}

	index,_,isLeader := kv.rf.Start(op)
	if isLeader {
		kv.mu.Lock()
		_,ok := kv.cNotifyChan[index]
		if !ok{
			kv.cNotifyChan[index] = make(chan Op, 1)
		}
		kv.mu.Unlock()

		select{
		case rep := <- kv.cNotifyChan[index]:
			if rep == op{
				reply.WrongLeader = false
				reply.Err = OK

				kv.mu.Lock()
				reply.Value = kv.kvs[args.Key]
				kv.mu.Unlock()
			}else{
				reply.Err = ""
			}
		case <-time.After(time.Duration(500)*time.Millisecond):
			reply.Err = TimeOut
		}
	}

	kv.mu.Lock()
	delete(kv.cNotifyChan, index)
	kv.mu.Unlock()

	DPrintf(fmt.Sprintf("Server %d Get %v with reply %v",kv.me, *args, *reply))
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	reply.WrongLeader = true
	reply.Err = ""

	if args.Gid != kv.gid{
		reply.Err = ErrWrongGroup
		return
	}

	op := Op{Key:args.Key,Value:args.Value,Cid:args.Cid,Seq:args.Seq,Action:args.Op,Gid:args.Gid,Shard:args.Shard}

	index,_,isLeader := kv.rf.Start(op)
	if isLeader{
		kv.mu.Lock()
		_,ok := kv.cNotifyChan[index]
		if !ok{
			kv.cNotifyChan[index] = make(chan Op, 1)
		}
		kv.mu.Unlock()

		select{
		case rep := <- kv.cNotifyChan[index]:
			if rep == op{
				reply.WrongLeader = false
				reply.Err = OK
			}else{
				reply.Err = ""
			}
		case <-time.After(500*time.Millisecond):
			reply.Err = TimeOut
		}
	}

	kv.mu.Lock()
	delete(kv.cNotifyChan, index)
	kv.mu.Unlock()

	DPrintf(fmt.Sprintf("Server %d PutAppend %v with reply %v",kv.me,*args, *reply))
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}


//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots with
// persister.SaveSnapshot(), and Raft should save its state (including
// log) with persister.SaveRaftState().
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// Your initialization code here.
	kv.kvs = make(map[string]string)
	kv.cNotifyChan = make(map[int]chan Op)
	kv.oldRequests = make(map[int64]int64)
	kv.sm = shardmaster.MakeClerk(masters)

	go kv.configDaemon()
	go kv.workerDaemon()

	return kv
}

func (kv *ShardKV) configDaemon(){
	for {
		select {
		case <- time.After(time.Duration(100)*time.Millisecond):
			conf := kv.sm.Query(-1)

			kv.mu.Lock()
			kv.config = conf
			kv.mu.Unlock()
		}
	}
}

func (kv *ShardKV) workerDaemon(){
	for {
		select {
		case rep := <-kv.applyCh:
			if rep.UseSnapshot {
				var lastIndex int
				var lastTerm int

				r := bytes.NewBuffer(rep.Snapshot)
				d := gob.NewDecoder(r)

				kv.mu.Lock()

				d.Decode(&lastIndex)
				d.Decode(&lastTerm)
				kv.kvs = make(map[string]string)
				kv.oldRequests = make(map[int64]int64)
				d.Decode(&kv.kvs)
				d.Decode(&kv.oldRequests)

				kv.mu.Unlock()
			}else {
				msg, ok:= rep.Command.(Op)
				if ok{
					kv.mu.Lock()

					// execute command
					if msg.Seq > kv.oldRequests[msg.Cid]{
						if msg.Action == "Put" {
							kv.kvs[msg.Key] = msg.Value
						}else if msg.Action == "Append" {
							kv.kvs[msg.Key] += msg.Value
						}else{
						}

						kv.oldRequests[msg.Cid] = msg.Seq
					}

					// send msg to wake up client wait
					index := rep.Index
					channel, ok := kv.cNotifyChan[index]
					if !ok {
						channel = make(chan Op, 1)
						kv.cNotifyChan[index] = channel
					}else{
						channel <- msg
					}

					// check if snapshot
					if kv.maxraftstate > 0 && kv.rf.GetRaftLogSize() > kv.maxraftstate {
						w := new(bytes.Buffer)
						e := gob.NewEncoder(w)
						e.Encode(kv.kvs)
						e.Encode(kv.oldRequests)
						data := w.Bytes()
						go kv.rf.DoSnapshot(data, rep.Index)
					}

					kv.mu.Unlock()
				}
			}
		}
	}
}
