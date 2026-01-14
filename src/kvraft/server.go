package raftkv

import (
	"encoding/gob"
	"log"
	"src/labrpc"
	"src/raft"
	"sync"
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
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	OpType string
	Key    string
	Value  string

	ClientId  int64
	RequestId int
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data           map[string]string // applied commands
	commitChannels map[int]chan Op   // channels that holds pending operations
	applied        map[int64]int     // request ids for each client
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	// set up start operatino
	op := Op{
		OpType:    "Get",
		Key:       args.Key,
		ClientId:  args.ClientId,
		RequestId: args.RequestId}

	// start operation
	i, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	cont := true
	kv.mu.Lock()
	channel, err := kv.commitChannels[i]
	if !err { // make channel is not exist
		channel = make(chan Op, 1)
		kv.commitChannels[i] = channel
	}
	kv.mu.Unlock()

	select { // wait for response of timeout
	case <-time.After(500 * time.Millisecond):
		cont = false
	case operation := <-channel:
		cont = operation == op
	}

	if !cont {
		reply.WrongLeader = true
		return
	}

	// update request id
	kv.mu.Lock()
	val, ok := kv.data[args.Key]
	if !ok {
		reply.Err = ErrNoKey
	} else {
		kv.applied[args.ClientId] = args.RequestId
		reply.Err = OK
		reply.Value = val
	}
	kv.mu.Unlock()
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	// set up operation
	op := Op{
		OpType:    args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		RequestId: args.RequestId}

	// start operation
	i, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}

	cont := true
	kv.mu.Lock()
	channel, ok := kv.commitChannels[i]
	if !ok { // make channel is not exist
		channel = make(chan Op, 1)
		kv.commitChannels[i] = channel
	}
	kv.mu.Unlock()

	select { // wait for response or timeout
	case <-time.After(500 * time.Millisecond): // at least one period of timeout/election
		cont = false
	case operation := <-channel:
		cont = operation == op
	}

	if !cont { // timed out -> wrong leader
		reply.WrongLeader = true
	} else {
		reply.Err = OK
	}

}

// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

// infinitely listen to apply channel
func (kv *RaftKV) ListenChannels() {
	go func() {
		for {
			// listen for messages
			appMsg := <-kv.applyCh
			command := appMsg.Command.(Op)

			kv.mu.Lock()
			val, err := kv.applied[command.ClientId]
			if !err || val < command.RequestId { // have not requested yet
				kv.applied[command.ClientId] = command.RequestId
				if command.OpType == "Append" {
					kv.data[command.Key] += command.Value
				} else if command.OpType == "Put" {
					kv.data[command.Key] = command.Value
				}
			}

			// set up commit channel if needed
			channel, ok := kv.commitChannels[appMsg.Index]
			if ok {
				channel <- command // add command to channel
			} else {
				kv.commitChannels[appMsg.Index] = make(chan Op, 1) // make channel
			}
			kv.mu.Unlock()
		}
	}()
}

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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// Your initialization code here.

	kv.data = make(map[string]string)
	kv.applied = make(map[int64]int)
	kv.commitChannels = make(map[int]chan Op)

	kv.ListenChannels()

	return kv
}
