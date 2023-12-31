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

import (
	"6.5840/labgob"
	"bytes"
	"fmt"
	"log"
	"os"
	"strconv"

	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// Retrieve the verbosity level from an environment variable
func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

type logTopic string

const (
	dClient  logTopic = "CLNT"
	dCommit  logTopic = "CMIT"
	dDrop    logTopic = "DROP"
	dError   logTopic = "ERRO"
	dInfo    logTopic = "INFO"
	dLeader  logTopic = "LEAD"
	dLog     logTopic = "LOG1"
	dLog2    logTopic = "LOG2"
	dPersist logTopic = "PERS"
	dSnap    logTopic = "SNAP"
	dTerm    logTopic = "TERM"
	dTest    logTopic = "TEST"
	dTimer   logTopic = "TIMR"
	dTrace   logTopic = "TRCE"
	dVote    logTopic = "VOTE"
	dWarn    logTopic = "WARN"
)

var debugStart time.Time
var debugVerbosity int

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func debug(topic logTopic, format string, a ...interface{}) {
	if debugVerbosity >= 1 {
		currentTime := time.Since(debugStart).Microseconds()
		currentTime /= 100
		prefix := fmt.Sprintf("%06d %v ", currentTime, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}

type State int

const (
	LEADER State = iota
	FOLLOWER
	CANDIDATE
)

const HEARTBEAT_INTERVAL = 50 * time.Millisecond

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state State
	// electionTimer time.Timer

	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// channels to replace timer
	// applyCh     chan ApplyMsg
	applierCh   chan ApplyMsg
	heartbeatCh chan struct{}
	grantVoteCh chan struct{}
	winElectCh  chan struct{}
	killCh      chan struct{}

	// snapshot
	snapshot          []byte
	lastIncludedIndex int
	lastIncludedTerm  int
}

func randomElectionTimeout() time.Duration {
	res := time.Duration(400+rand.Int63n(200)) * time.Millisecond
	// debug(dTimer, "randomElectionTimeout: %vms", res.Milliseconds())
	return res
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	debug(dTerm, "S%v GetState: term %v, isLeader %v", rf.me, rf.currentTerm, rf.state == LEADER)
	return rf.currentTerm, rf.state == LEADER
}

func (rf *Raft) isLeader() bool {
	_, isLeader := rf.GetState()
	return isLeader
}

// should be called with lock held
func (rf *Raft) getLastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) getLastLogIndex() int {
	return rf.log[len(rf.log)-1].Index
}

// get an entry with its real index
func (rf *Raft) entry(index int) LogEntry {
	if index-rf.lastIncludedIndex < 0 || index-rf.lastIncludedIndex >= len(rf.log) {
		return LogEntry{Index: -1, Term: -1}
	}
	return rf.log[index-rf.lastIncludedIndex]
}

// switch real index to log index
func (rf *Raft) logIndex(realIndex int) int {
	return realIndex - rf.lastIncludedIndex
}

// switch log index to real index
func (rf *Raft) realIndex(logIndex int) int {
	return logIndex + rf.lastIncludedIndex
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	_ = e.Encode(rf.currentTerm)
	_ = e.Encode(rf.votedFor)
	_ = e.Encode(rf.log)
	_ = e.Encode(rf.lastIncludedTerm)
	_ = e.Encode(rf.lastIncludedIndex)
	state := w.Bytes()
	rf.persister.Save(state, rf.snapshot)

	// debug(dPersist, "S%v persist: term %v, votedFor %v, log %v", rf.me, rf.currentTerm, rf.votedFor, rf.log)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	_ = d.Decode(&rf.currentTerm)
	_ = d.Decode(&rf.votedFor)
	_ = d.Decode(&rf.log)
	_ = d.Decode(&rf.lastIncludedTerm)
	_ = d.Decode(&rf.lastIncludedIndex)

	debug(dPersist, "S%v readPersist: term %v, votedFor %v, log %v, lastIncludedTerm %v, lastIncludedIndex %v", rf.me, rf.currentTerm, rf.votedFor, rf.log, rf.lastIncludedTerm, rf.lastIncludedIndex)

	if rf.persister.SnapshotSize() > 0 {
		rf.snapshot = rf.persister.ReadSnapshot()
		rf.applierCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      rf.snapshot,
			SnapshotTerm:  rf.lastIncludedTerm,
			SnapshotIndex: rf.lastIncludedIndex,
		}
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	debug(dSnap, "S%v receive snapshot, index %v", rf.me, index)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if index <= rf.lastIncludedIndex {
		// outdated request
		return
	}
	rf.snapshot = snapshot
	debug(dSnap, "S%v starting snapshot, slice from %v", rf.me, rf.logIndex(index))
	rf.log = append([]LogEntry{{Index: index, Term: rf.getLastLogTerm()}}, rf.log[rf.logIndex(index)+1:]...)
	rf.lastIncludedTerm = rf.getLastLogTerm()
	rf.lastIncludedIndex = index
	debug(dSnap, "S%v snapshot done, log %v", rf.me, rf.log)
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int

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

type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Success bool
	Term    int

	// optimization: next index to try if not successful
	NextTryIndex int
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term < rf.currentTerm {
		// Reply false if term < currentTerm
		debug(dVote, "S%v receive lower term %v from S%v, reject vote as %v", rf.me, args.Term, args.CandidateId, rf.currentTerm)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		// If RPC request contains term T > currentTerm:
		// set currentTerm = T, convert to follower
		debug(dVote, "S%v receive higher term %v from S%v RequestVote, convert to follower from %v", rf.me, args.Term, args.CandidateId, rf.currentTerm)
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = FOLLOWER
	}

	// If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && (args.LastLogTerm > rf.getLastLogTerm() ||
		(args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex >= rf.getLastLogIndex())) {
		debug(dVote, "S%v vote for S%v", rf.me, args.CandidateId)
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.votedFor = args.CandidateId
		rf.grantVoteCh <- struct{}{}
	} else {
		debug(dVote, "S%v reject vote for S%v", rf.me, args.CandidateId)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term < rf.currentTerm {
		// Reply false if term < currentTerm
		debug(dLog, "S%v receive lower term from S%v, reject AppendEntries", rf.me, args.LeaderId)
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		// If RPC request contains term T > currentTerm:
		// set currentTerm = T, convert to follower
		debug(dLog, "S%v receive higher term from S%v, convert to follower", rf.me, args.LeaderId)
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = FOLLOWER
	}

	if args.Term == rf.currentTerm && rf.state == CANDIDATE {
		debug(dLog, "S%v receive same term from S%v, convert to follower", rf.me, args.LeaderId)
		rf.state = FOLLOWER
	}

	// received AppendEntries RPC from current leader, reset election timer
	debug(dLog, "S%v receive AppendEntries from S%v, reset election timer", rf.me, args.LeaderId)
	rf.heartbeatCh <- struct{}{}

	reply.Success = false
	reply.Term = rf.currentTerm

	if args.PrevLogIndex > rf.getLastLogIndex() {
		// tell leader to retry with smaller prevLogIndex
		reply.NextTryIndex = rf.getLastLogIndex() + 1
		debug(dLog, "S%v receive AppendEntries from S%v, prevLogIndex %v > lastLogIndex %v, reject AppendEntries, NextTryIndex %v", rf.me, args.LeaderId, args.PrevLogIndex, rf.getLastLogIndex(), reply.NextTryIndex)
		return
	}

	if args.PrevLogIndex >= rf.lastIncludedIndex && args.PrevLogTerm != rf.entry(args.PrevLogIndex).Term {
		// if entry log[prevLogIndex] conflicts with new one, there may be conflict entries before.
		// we can bypass all entries during the problematic term to speed up.
		debug(dLog, "S%v receive AppendEntries from S%v, prevLogTerm %v != lastLogTerm %v, reject AppendEntries", rf.me, args.LeaderId, args.PrevLogTerm, rf.entry(args.PrevLogIndex).Term)
		term := rf.entry(args.PrevLogIndex).Term
		for i := args.PrevLogIndex - 1; i >= rf.lastIncludedIndex && rf.entry(i).Term == term; i-- {
			reply.NextTryIndex = i + 1
		}
	} else if args.PrevLogIndex >= rf.lastIncludedIndex-1 {
		// otherwise log up to prevLogIndex are safe.
		// we can merge local log and entries from leader, and apply log if commitIndex changes.
		debug(dLog, "S%v receive AppendEntries from S%v, prevLogTerm %v, prevLogIndex %v, merge log", rf.me, args.LeaderId, args.PrevLogTerm, args.PrevLogIndex)
		// var restLog []LogEntry
		// restLog = rf.log[args.PrevLogIndex+1-rf.lastIncludedIndex:]
		rf.log = append(rf.log[:args.PrevLogIndex+1-rf.lastIncludedIndex], args.Entries...)
		debug(dLog, "S%v merge log done, LastLogIndex %v", rf.me, rf.getLastLogIndex())

		reply.Success = true
		reply.NextTryIndex = args.PrevLogIndex

		if rf.commitIndex < args.LeaderCommit {
			// update commitIndex and apply log
			debug(dLog, "S%v receive AppendEntries from S%v, update commitIndex from %v to %v", rf.me, args.LeaderId, rf.commitIndex, args.LeaderCommit)
			rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
			go rf.applyLog()
		}
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		// Reply false if term < currentTerm
		debug(dSnap, "S%v receive lower term from S%v, reject InstallSnapshot", rf.me, args.LeaderId)
		reply.Term = rf.currentTerm
		return
	}

	defer rf.persist()

	if args.Term > rf.currentTerm {
		// If RPC request contains term T > currentTerm:
		// set currentTerm = T, convert to follower
		debug(dSnap, "S%v receive higher term from S%v, convert to follower", rf.me, args.LeaderId)
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = FOLLOWER
	}

	if args.Term == rf.currentTerm && rf.state == CANDIDATE {
		debug(dSnap, "S%v receive same term from S%v, convert to follower", rf.me, args.LeaderId)
		rf.state = FOLLOWER
	}

	// received InstallSnapshot RPC from current leader, reset election timer
	debug(dSnap, "S%v receive InstallSnapshot from S%v, reset election timer", rf.me, args.LeaderId)
	rf.heartbeatCh <- struct{}{}

	reply.Term = rf.currentTerm

	if (args.LastIncludedIndex < rf.lastIncludedIndex) || (rf.lastIncludedIndex == args.LastIncludedIndex && rf.lastIncludedTerm == args.LastIncludedTerm) {
		// discard snapshot if it is outdated
		debug(dSnap, "S%v receive InstallSnapshot from S%v, args.lastIncludedIndex %v, rf.lastIncludedIndex %v, args.LastIncludedTerm %v, rf.lastIncludedTerm %v, discard snapshot", rf.me, args.LeaderId, args.LastIncludedIndex, rf.lastIncludedIndex, args.LastIncludedTerm, rf.lastIncludedTerm)
		return
	}

	rf.snapshot = args.Data

	debug(dSnap, "S%v receive InstallSnapshot from S%v, lastIncludedIndex %v, lastIncludedTerm %v", rf.me, args.LeaderId, args.LastIncludedIndex, args.LastIncludedTerm)
	// for i := 1; i < len(rf.log); i++ {
	// 	if rf.realIndex(i) == args.LastIncludedIndex && rf.log[i].Term == args.LastIncludedTerm {
	// 		debug(dSnap, "S%v receive InstallSnapshot from S%v, find same entry at index %v, term %v", rf.me, args.LeaderId, rf.realIndex(i), rf.log[i].Term)
	// 		rf.lastIncludedTerm = args.LastIncludedTerm
	// 		rf.lastIncludedIndex = args.LastIncludedIndex
	// 		rf.log = append([]LogEntry{{Index: args.LastIncludedIndex, Term: rf.lastIncludedTerm}}, rf.log[i+1:]...)
	// 		rf.lastApplied = rf.lastIncludedIndex
	// 		rf.commitIndex = rf.lastIncludedIndex
	// 		return
	// 	}
	// }
	debug(dSnap, "S%v receive InstallSnapshot from S%v, cannot find same entry, install the snapshot", rf.me, args.LeaderId)
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.log = []LogEntry{{Index: args.LastIncludedIndex, Term: args.LastIncludedTerm}}
	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex

	msg := ApplyMsg{}
	msg.SnapshotValid = true
	msg.Snapshot = args.Data
	msg.SnapshotTerm = args.LastIncludedTerm
	msg.SnapshotIndex = args.LastIncludedIndex
	// rf.applyCh <- msg
	rf.applierCh <- msg
	debug(dSnap, "S%v apply snapshot", rf.me)
}

func (rf *Raft) applyLog() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		if rf.entry(i).Index <= 0 || rf.entry(i).Command == nil {
			continue
		}
		debug(dCommit, "S%v apply log %v, %v, term %v", rf.me, rf.entry(i).Index, rf.entry(i).Command, rf.entry(i).Term)
		msg := ApplyMsg{}
		msg.CommandIndex = rf.entry(i).Index
		msg.CommandValid = true
		msg.Command = rf.entry(i).Command
		rf.applierCh <- msg
		debug(dCommit, "S%v apply log %v done", rf.me, rf.entry(i).Index)
	}
	rf.lastApplied = rf.commitIndex
}

func (rf *Raft) applier(applyCh chan ApplyMsg) {
	for {
		select {
		case <-rf.killCh:
			return
		case msg := <-rf.applierCh:
			// debug(dCommit, "S%v applier: %v", rf.me, msg)
			applyCh <- msg
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
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
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
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	term, index := -1, -1
	isLeader := rf.state == LEADER

	if isLeader {
		term = rf.currentTerm
		index = rf.getLastLogIndex() + 1
		debug(dClient, "S%v receive command %v from client during term %v, set Index %v", rf.me, command, rf.currentTerm, index)
		rf.log = append(rf.log, LogEntry{Index: index, Term: term, Command: command})
	}

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	debug(dInfo, "S%v killed", rf.me)
	rf.killCh <- struct{}{}
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) preElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	rf.state = CANDIDATE
	rf.currentTerm++
	rf.votedFor = rf.me
}

func (rf *Raft) startElection() {
	args := RequestVoteArgs{}
	rf.mu.RLock()
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = rf.getLastLogIndex()
	args.LastLogTerm = rf.getLastLogTerm()
	rf.mu.RUnlock()

	debug(dVote, "S%v start election, currentTerm: %v", rf.me, args.Term)

	// send RequestVote RPCs to all other servers
	// and collect votes via channel
	voteCh := make(chan bool, len(rf.peers)-1)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			reply := RequestVoteReply{}
			// debug(dVote, "S%v send RequestVote to S%v", rf.me, i)
			// startTime := time.Now()
			if ok := rf.sendRequestVote(i, &args, &reply); !ok {
				// debug(dVote, "S%v send RequestVote to S%v failed", rf.me, i)
				voteCh <- false
				return
			}
			// debug(dVote, "S%v receive reply from S%v", rf.me, i)
			// debug(dVote, "S%v receive reply from S%v after %vus", rf.me, i, time.Since(startTime).Microseconds())
			if reply.Term > args.Term {
				// If RPC response contains term T > currentTerm:
				// set currentTerm = T, convert to follower
				rf.mu.Lock()
				debug(dVote, "S%v receive higher term %v from S%v, convert to follower from %v", rf.me, reply.Term, i, rf.currentTerm)
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.state = FOLLOWER
				rf.persist()
				rf.mu.Unlock()

				// ensure that voteCh is not blocked
				voteCh <- false
			} else {
				debug(dVote, "S%v receive vote from S%v: %v", rf.me, i, reply.VoteGranted)
				voteCh <- reply.VoteGranted
			}
		}(i)
	}

	// count votes
	votes := 1
	for voteGranted := range voteCh {
		if voteGranted {
			votes++
		}
		rf.mu.RLock()
		if rf.state != CANDIDATE {
			debug(dVote, "S%v is no longer candidate", rf.me)
			rf.mu.RUnlock()
			return
		}
		rf.mu.RUnlock()
		if votes > len(rf.peers)/2 {
			debug(dLeader, "S%v becomes leader", rf.me)
			rf.mu.Lock()
			rf.state = LEADER
			rf.nextIndex = make([]int, len(rf.peers))
			rf.matchIndex = make([]int, len(rf.peers))
			nextIndex := rf.getLastLogIndex() + 1
			for i := range rf.nextIndex {
				rf.nextIndex[i] = nextIndex
			}
			rf.persist()
			rf.mu.Unlock()
			rf.winElectCh <- struct{}{}
			return
		}
	}
}

func (rf *Raft) broadcast() {
	debug(dLeader, "S%v start broadcasting heartbeat", rf.me)

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			// debug(dLeader, "S%v send heartbeat to S%v", rf.me, i)
			rf.mu.RLock()
			if rf.state != LEADER {
				rf.mu.RUnlock()
				return
			}

			if rf.nextIndex[i] > rf.lastIncludedIndex {
				args := AppendEntriesArgs{}
				reply := AppendEntriesReply{}

				args.LeaderId = rf.me
				args.Term = rf.currentTerm
				args.PrevLogIndex = rf.nextIndex[i] - 1
				if args.PrevLogIndex >= rf.lastIncludedIndex {
					args.PrevLogTerm = rf.entry(args.PrevLogIndex).Term
				}
				if rf.nextIndex[i] <= rf.getLastLogIndex() {
					args.Entries = make([]LogEntry, len(rf.log[rf.logIndex(rf.nextIndex[i]):]))
					copy(args.Entries[:], rf.log[rf.logIndex(rf.nextIndex[i]):])
				}
				args.LeaderCommit = rf.commitIndex
				debug(dLeader, "S%v (term %v) send AppendEntries to S%v, prevLogIndex %v, prevLogTerm %v, entries %v, commitIndex %v", rf.me, args.Term, i, args.PrevLogIndex, args.PrevLogTerm, args.Entries, args.LeaderCommit)
				debug(dLeader, "S%v (term %v) send AppendEntries to S%v, lastIncludedIndex %v, lastIncludedTerm %v", rf.me, args.Term, i, rf.lastIncludedIndex, rf.lastIncludedTerm)
				rf.mu.RUnlock()

				go func() {
					if ok := rf.sendAppendEntries(i, &args, &reply); !ok {
						// debug(dLeader, "S%v send heartbeat to S%v failed", rf.me, i)
						return
					}
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						debug(dLeader, "S%v receive higher term %v from S%v during heartbeat, convert to follower from %v", rf.me, reply.Term, i, rf.currentTerm)
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.state = FOLLOWER
						rf.mu.Unlock()
						return
					}

					if reply.Success {
						rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries)
						rf.nextIndex[i] = rf.matchIndex[i] + 1
						debug(dLeader, "S%v update matchIndex[%v] to %v, nextIndex[%v] to %v", rf.me, i, rf.matchIndex[i], i, rf.nextIndex[i])
					} else {
						rf.nextIndex[i] = reply.NextTryIndex
						debug(dLeader, "S%v update nextIndex[%v] to %v", rf.me, i, rf.nextIndex[i])
					}

					for N := rf.getLastLogIndex(); N > rf.commitIndex && rf.entry(N).Term == rf.currentTerm; N-- {
						// find if there exists an N to update commitIndex
						count := 1
						for i := range rf.peers {
							if i != rf.me && rf.matchIndex[i] >= N {
								count++
							}
						}
						if count > len(rf.peers)/2 {
							rf.commitIndex = N
							debug(dLeader, "S%v update commitIndex to %v", rf.me, rf.commitIndex)
							go rf.applyLog()
							break
						}
					}

					rf.mu.Unlock()
				}()
			} else {
				// send snapshot
				args := InstallSnapshotArgs{
					Term:              rf.currentTerm,
					LeaderId:          rf.me,
					LastIncludedIndex: rf.lastIncludedIndex,
					LastIncludedTerm:  rf.lastIncludedTerm,
					Data:              rf.snapshot,
				}
				reply := InstallSnapshotReply{}
				rf.mu.RUnlock()
				debug(dLeader, "S%v (term %v) send InstallSnapshot to S%v, lastIncludedIndex %v, lastIncludedTerm %v", rf.me, args.Term, i, args.LastIncludedIndex, args.LastIncludedTerm)

				go func() {
					if ok := rf.sendInstallSnapshot(i, &args, &reply); !ok {
						// debug(dLeader, "S%v send snapshot to S%v failed", rf.me, i)
						return
					}
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						debug(dLeader, "S%v receive higher term %v from S%v during snapshot, convert to follower from %v", rf.me, reply.Term, i, rf.currentTerm)
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.state = FOLLOWER
						rf.mu.Unlock()
						return
					}

					rf.nextIndex[i] = rf.lastIncludedIndex + 1
					rf.matchIndex[i] = rf.lastIncludedIndex
					rf.mu.Unlock()
				}()
			}
		}(i)
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (2A)
		// Check if a leader election should be started.

		rf.mu.RLock()
		state := rf.state
		rf.mu.RUnlock()

		switch state {
		case FOLLOWER:
			debug(dInfo, "S%v ticking as FOLLOWER", rf.me)
			select {
			case <-rf.killCh:
			case <-rf.heartbeatCh:
				debug(dTimer, "S%v receive heartbeat as FOLLOWER, reset election timer", rf.me)
			case <-rf.grantVoteCh:
				debug(dTimer, "S%v voted, reset election timer as FOLLOWER", rf.me)
			case <-time.After(randomElectionTimeout()):
				debug(dTimer, "S%v election timer expired as FOLLOWER", rf.me)
				rf.preElection()
				go rf.startElection()
			}
		case CANDIDATE:
			debug(dInfo, "S%v ticking as CANDIDATE", rf.me)
			select {
			case <-rf.killCh:
			case <-rf.winElectCh:
				debug(dTimer, "S%v win election as CANDIDATE", rf.me)
			case <-rf.heartbeatCh:
				debug(dTimer, "S%v receive heartbeat as CANDIDATE, convert to follower", rf.me)
			case <-time.After(randomElectionTimeout()):
				debug(dTimer, "S%v election timer expired as CANDIDATE", rf.me)
				rf.preElection()
				go rf.startElection()
			}
		case LEADER:
			debug(dInfo, "S%v ticking as LEADER", rf.me)
			go rf.broadcast()
			select {
			case <-rf.killCh:
			case <-rf.heartbeatCh:
			case <-time.After(HEARTBEAT_INTERVAL):
			}
		}
	}
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
	rf.state = FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = append(rf.log, LogEntry{Term: -1})

	rf.commitIndex = 0
	rf.lastApplied = 0

	// rf.applyCh = applyCh
	rf.applierCh = make(chan ApplyMsg, 100)
	rf.heartbeatCh = make(chan struct{}, 100)
	rf.grantVoteCh = make(chan struct{}, 100)
	rf.winElectCh = make(chan struct{}, 100)
	rf.killCh = make(chan struct{}, 100)

	rf.snapshot = nil
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.persist()

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier(applyCh)

	return rf
}
