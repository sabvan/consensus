/* DONT SUBMIT UNEDITED */

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

	Type  string
	Key   string
	Value string
	// duplicate detection info needs to be part of state machine
	// so that all raft servers eliminate the same duplicates
	ClientId  int64
	RequestId int64
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	store       map[string]string
	results     map[int]chan Op
	lastApplied map[int64]int64
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	op := Op{
		Type:      "Get",
		Key:       args.Key,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	ok, appliedOp := kv.waitForApplied(op)
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}

	reply.Err = OK
	reply.Value = appliedOp.Value
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Type:      args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	ok, _ := kv.waitForApplied(op)
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}

	reply.Err = OK

}

func (kv *RaftKV) waitForApplied(op Op) (bool, Op) {
	index, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		return false, op
	}

	kv.mu.Lock()
	opCh, ok := kv.results[index]
	if !ok {
		opCh = make(chan Op, 1)
		kv.results[index] = opCh
	}
	kv.mu.Unlock()

	select {
	case appliedOp := <-opCh:
		return kv.isSameOp(op, appliedOp), appliedOp
	case <-time.After(600 * time.Millisecond):
		return false, op
	}
}

func (kv *RaftKV) isSameOp(issued Op, applied Op) bool {
	return issued.ClientId == applied.ClientId &&
		issued.RequestId == applied.RequestId
}

func (kv *RaftKV) applyOpsLoop() {
	for {
		msg := <-kv.applyCh

		index := msg.Index
		op := msg.Command.(Op)

		kv.mu.Lock()

		if op.Type == "Get" {
			kv.applyToStateMachine(&op)
		} else {
			lastId, ok := kv.lastApplied[op.ClientId]
			if !ok || op.RequestId > lastId {
				kv.applyToStateMachine(&op)
				kv.lastApplied[op.ClientId] = op.RequestId
			}
		}

		opCh, ok := kv.results[index]
		if !ok {
			opCh = make(chan Op, 1)
			kv.results[index] = opCh
		}
		opCh <- op

		kv.mu.Unlock()
	}
}

func (kv *RaftKV) applyToStateMachine(op *Op) {
	switch op.Type {
	case "Get":
		op.Value = kv.store[op.Key]
	case "Put":
		kv.store[op.Key] = op.Value
	case "Append":
		kv.store[op.Key] += op.Value
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

	// Your initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg, 1)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.store = make(map[string]string)
	kv.results = make(map[int]chan Op)
	kv.lastApplied = make(map[int64]int64)

	// You may need initialization code here.
	go kv.applyOpsLoop()

	return kv
}
