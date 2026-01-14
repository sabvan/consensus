package raft

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

import (
	// "context"
	// "fmt"
	// "bytes"
	"math/rand"
	"src/labrpc"
	"sync"
	"time"
)

import "bytes"
import "encoding/gob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

/* type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
} */

const (
	Follower = iota
	Candidate
	Leader
)

const ElectionTimeout = 300
const HeartBeat = 50

func randTimeout() time.Duration {
	randTimeout := ElectionTimeout + rand.Intn(ElectionTimeout)
	return time.Duration(randTimeout) * time.Millisecond
}

type CommandTerm struct {
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	status      int
	applyMsg    chan ApplyMsg
	currentTerm int
	votedFor    int
	log         []CommandTerm
	commitIndex int
	// lastApplied  int
	lastLogIndex int
	nextIndex    []int
	matchIndex   []int
	lastAccessed time.Time
}

// example RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// ** ADDED STRUCT **
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entry        []CommandTerm
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	Xterm   int
	XIndex  int
	XLen    int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.status == Leader

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	currentTerm, votedFor := 0, 0
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&rf.log) != nil {

	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.lastLogIndex = len(rf.log) - 1
	}
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm == args.Term && rf.votedFor == args.CandidateId {
		reply.VoteGranted, reply.Term = true, rf.currentTerm
		return
	}
	if rf.currentTerm > args.Term ||
		(rf.currentTerm == args.Term && rf.votedFor != -1) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, -1
		rf.status = Follower
	}
	reply.Term = args.Term
	if rf.lastLogIndex-1 >= 0 {
		lastLogTerm := rf.log[rf.lastLogIndex-1].Term
		if lastLogTerm > args.LastLogTerm ||
			(lastLogTerm == args.LastLogTerm && rf.lastLogIndex > args.LastLogIndex) {
			reply.VoteGranted = false
			return
		}
	}
	rf.status = Follower
	rf.lastAccessed = time.Now()
	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	rf.persist()

}

func (rf *Raft) AppendEntriesHandler(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// if a leader, revert back to follower if term >= ours
	// accept and update term

	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Xterm = -1
	reply.XIndex = -1
	reply.XLen = len(rf.log)
	reply.Success = false
	reply.Term = rf.currentTerm
	// if term too old reject
	if args.Term < rf.currentTerm {
		return
	}

	// if length of our log is smaller then reject
	// ours is def not updated enough
	if len(rf.log) < args.PrevLogIndex+1 {
		return
	}

	// if terms are different at the same index
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// return the term that is at that index
		reply.Xterm = rf.log[args.PrevLogIndex].Term
		// go through log and return latest match index where terms match
		for i, v := range rf.log {
			if v.Term == reply.Xterm {
				reply.XIndex = i
				break
			}
		}
		return
	}

	// truncate list if no matching
	index := 0
	// for each entry in args log input
	for ; index < len(args.Entry); index++ {
		currentIndex := args.PrevLogIndex + 1 + index
		// if index is too big, reject
		if currentIndex > len(rf.log)-1 {
			break
		}
		// the term doesnt match, truncate our log
		if rf.log[currentIndex].Term != args.Entry[index].Term {
			rf.log = rf.log[:currentIndex]
			rf.lastLogIndex = len(rf.log) - 1
			rf.persist()
			break
		}
	}

	reply.Success = true
	rf.lastAccessed = time.Now()

	// append to our olog
	if len(args.Entry) > 0 {
		rf.log = append(rf.log, args.Entry[index:]...)
		rf.lastLogIndex = len(rf.log) - 1
		rf.persist()
	}
	// if their commit is later than our commit,
	// change out commit index to be the min
	// why are we applying message?
	if args.LeaderCommit > rf.commitIndex {
		min := min(args.LeaderCommit, rf.lastLogIndex)
		for i := rf.commitIndex + 1; i <= min; i++ {
			rf.commitIndex = i
			rf.applyMsg <- ApplyMsg{
				Command: rf.log[i].Command,
				Index:   i,
			}
		}
	}
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntriesHandler", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader = rf.status == Leader
	if !isLeader {
		return 0, 0, false
	}

	term = rf.currentTerm
	rf.log = append(rf.log, CommandTerm{
		Command: command,
		Term:    term,
	})
	rf.lastLogIndex = len(rf.log) - 1
	index = rf.lastLogIndex
	rf.persist()
	return index, term, isLeader
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.status = Follower
	rf.log = []CommandTerm{
		{
			Command: nil,
			Term:    0,
		},
	}
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.applyMsg = applyCh
	rf.lastLogIndex = 0
	rf.commitIndex = 0
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.manageLifecycle()

	return rf
}

func (rf *Raft) manageFollower() {
	duration := randTimeout()
	time.Sleep(duration)
	rf.mu.Lock()
	lastAccessed := rf.lastAccessed
	rf.mu.Unlock()
	if time.Since(lastAccessed) >= duration {
		rf.mu.Lock()
		rf.status = Candidate
		rf.currentTerm++
		rf.votedFor = -1
		rf.persist()
		rf.mu.Unlock()
	}
}

func (rf *Raft) manageCandidate() {
	timeOut := randTimeout()
	start := time.Now()
	rf.mu.Lock()
	peers := rf.peers
	me := rf.me
	term := rf.currentTerm
	lastLogIndex := rf.lastLogIndex
	lastLogTerm := rf.log[lastLogIndex].Term
	rf.mu.Unlock()
	count := 0
	total := len(peers)
	finished := 0
	majority := (total / 2) + 1
	for peer := range peers {
		if me == peer {
			rf.mu.Lock()
			count++
			finished++
			rf.mu.Unlock()
			continue
		}

		go func(peer int) {
			args := RequestVoteArgs{}
			args.Term = term
			args.CandidateId = me
			args.LastLogIndex = lastLogIndex
			args.LastLogTerm = lastLogTerm
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(peer, &args, &reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if !ok {
				finished++
				return
			}
			if reply.VoteGranted {
				finished++
				count++
			} else {
				finished++
				if args.Term < reply.Term {
					rf.status = Follower
					rf.persist()
				}
			}
		}(peer)
	}

	for {
		rf.mu.Lock()
		if count >= majority || finished == total || time.Since(start) >= timeOut {
			break
		}
		rf.mu.Unlock()
		time.Sleep(HeartBeat * time.Millisecond)
	}

	if time.Since(start) >= timeOut {
		rf.status = Follower
		// rf.persist()
		rf.mu.Unlock()
		return
	}

	if rf.status == Candidate && count >= majority {
		rf.status = Leader
		for peer := range peers {
			rf.nextIndex[peer] = rf.lastLogIndex + 1
		}
	} else {
		rf.status = Follower
	}
	rf.persist()
	rf.mu.Unlock()
}

func (rf *Raft) manageLeader() {

	rf.mu.Lock()
	me := rf.me
	term := rf.currentTerm
	commitIndex := rf.commitIndex
	peers := rf.peers
	nextIndex := rf.nextIndex

	lastLogIndex := rf.lastLogIndex
	matchIndex := rf.matchIndex
	nextIndex[me] = lastLogIndex + 1
	matchIndex[me] = lastLogIndex
	log := rf.log
	rf.mu.Unlock()

	// commit index advancement
	// move n up if majority of peers have replicated and send applyMsg
	// does not send RPCs
	for n := commitIndex + 1; n <= lastLogIndex; n++ {
		count := 0
		total := len(peers)
		majority := (total / 2) + 1
		for peer := range peers {
			if matchIndex[peer] >= n && log[n].Term == term {
				count++
			}
		}

		if count >= majority {
			rf.mu.Lock()
			i := rf.commitIndex + 1
			for ; i <= n; i++ {
				rf.applyMsg <- ApplyMsg{
					Command: log[i].Command,
					Index:   i,
				}
				rf.commitIndex = rf.commitIndex + 1
			}
			rf.mu.Unlock()
		}
	}

	// hearbeats
	// if failure, adjusts next index and match index
	for peer := range peers {
		if peer == me {
			continue
		}

		args := AppendEntriesArgs{}
		reply := AppendEntriesReply{}
		rf.mu.Lock()
		args.Term = rf.currentTerm
		prevLogIndex := nextIndex[peer] - 1
		args.PrevLogIndex = prevLogIndex
		args.PrevLogTerm = rf.log[prevLogIndex].Term
		args.LeaderCommit = rf.commitIndex
		args.LeaderId = rf.me
		// appends log to include all the new stuff
		if nextIndex[peer] <= lastLogIndex {
			args.Entry = rf.log[prevLogIndex+1 : lastLogIndex+1]
		}
		rf.mu.Unlock()

		go func(peer int) {
			ok := rf.sendAppendEntries(peer, &args, &reply)
			if !ok {
				return
			}

			rf.mu.Lock()
			// if succesfful increase netIndex to catch up to current
			// make match the same
			if reply.Success {
				rf.nextIndex[peer] = min(rf.nextIndex[peer]+len(args.Entry), rf.lastLogIndex+1)
				rf.matchIndex[peer] = prevLogIndex + len(args.Entry)
			} else {
				// if another leader revert
				if reply.Term > args.Term {
					rf.status = Follower
					rf.mu.Unlock()
					return
				}
				if reply.Xterm == -1 {
					rf.nextIndex[peer] = reply.XLen
					rf.mu.Unlock()
					return
				}
				index := -1
				for i, v := range rf.log {
					if v.Term == reply.Xterm {
						index = i
					}
				}
				if index == -1 {
					rf.nextIndex[peer] = reply.XIndex
				} else {
					rf.nextIndex[peer] = index
				}
			}
			rf.mu.Unlock()
		}(peer)
	}
}

func (rf *Raft) manageLifecycle() {
	for {
		rf.mu.Lock()
		status := rf.status
		rf.mu.Unlock()
		if status == Follower {
			rf.manageFollower()
		} else if status == Candidate {
			rf.manageCandidate()
		} else if status == Leader {
			rf.manageLeader()
		}
		time.Sleep(HeartBeat * time.Millisecond)
	}

	// time.Sleep(HeartBeat)

}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
