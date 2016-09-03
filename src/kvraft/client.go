package raftkv

import "labrpc"
import "crypto/rand"
import (
	"math/big"
	"sync"
)


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	cid int64
	seq int64
	mu sync.Mutex

	leader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.cid = nrand()
	ck.seq = 0
	ck.leader = -1
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{Key:key,Cid:ck.cid}

	ck.mu.Lock()
	ck.seq++
	args.Seq = ck.seq
	ck.mu.Unlock()

	for {
		if ck.leader != -1 {
			reply := GetReply{}
			ok := ck.servers[ck.leader].Call("RaftKV.Get", &args, &reply)
			if ok {
				if !reply.WrongLeader{
					return reply.Value
				}
			}
		}

		for i := range ck.servers{
			reply := GetReply{}
			ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
			if ok {
				if !reply.WrongLeader{
					ck.leader = i
					return reply.Value
				}
			}
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{Key:key,Value:value,Op:op,Cid:ck.cid}

	ck.mu.Lock()
	ck.seq++
	args.Seq = ck.seq
	ck.mu.Unlock()

	for {
		for i := range ck.servers{
			reply := PutAppendReply{}
			ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
			if ok {
				if !reply.WrongLeader{
					return
				}
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
