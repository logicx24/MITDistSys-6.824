package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"fmt"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	Key string
	Value string
	Cid int64
	Seq int64
	Action string
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvs map[string]string // key->value

	res map[int]chan Op
}


func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	op := Op{Key:args.Key,Value:"",Cid:args.Cid,Seq:args.Seq,Action:"Get"}

	index,_,isLeader := kv.rf.Start(op)
	if isLeader == false {
		reply.WrongLeader = true
		return
	}

	kv.mu.Lock()
	_,ok := kv.res[index]
	if !ok{
		kv.res[index] = make(chan Op)
	}
	kv.mu.Unlock()

	select{
	case rep := <- kv.res[index]:
		if rep == op{
			reply.WrongLeader = false

			kv.mu.Lock()
			reply.Value = kv.kvs[args.Key]
			kv.mu.Unlock()
		}else{
			reply.Err = Error
		}
	case <-time.After(time.Duration(500)*time.Millisecond):
		reply.Err = TimeOut
	}

	DPrintf(fmt.Sprintf("Get %v with reply %v",*args, *reply))
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{Key:args.Key,Value:args.Value,Cid:args.Cid,Seq:args.Seq,Action:args.Op}

	index,_,isLeader := kv.rf.Start(op)
	if isLeader == false {
		reply.WrongLeader = true
		return
	}

	kv.mu.Lock()
	_,ok := kv.res[index]
	if !ok{
		kv.res[index] = make(chan Op)
	}
	kv.mu.Unlock()

	select{
	case rep := <- kv.res[index]:
		if rep == op{
			reply.WrongLeader = false
		}else{
			reply.Err = Error
		}
	case <-time.After(500*time.Millisecond):
		reply.Err = TimeOut
	}

	DPrintf(fmt.Sprintf("PutAppend %v with reply %v",*args, *reply))
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// Your initialization code here.
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.kvs = make(map[string]string)
	kv.res = make(map[int]chan Op)

	go func(){
		for {
			select {
			case rep := <-kv.applyCh:
				kv.mu.Lock()

				msg := rep.Command.(Op)
				v, _ := kv.kvs[msg.Key]
				if msg.Action == "Put" {
					kv.kvs[msg.Key] = msg.Value
				}else if msg.Action == "Append" {
					kv.kvs[msg.Key] = v+msg.Value
				}else{
				}

				index := rep.Index
				channel, ok := kv.res[index]
				if !ok {
					channel = make(chan Op)
					kv.res[index] = channel
				}else{
					channel <- msg
				}

				kv.mu.Unlock()
			}
		}
	}()

	return kv
}
