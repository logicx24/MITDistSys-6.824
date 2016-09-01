package raftkv

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
	TimeOut  = "TimeOut"
	Error    = "Error"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Cid int64
	Seq int64
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Cid int64
	Seq int64
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}
