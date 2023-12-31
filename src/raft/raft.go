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

// func timerResetHelper(timer *time.Timer, interval time.Duration) {
// 	debug(dTimer, "timerResetHelper: %vms", interval.Milliseconds())
// 	timer = time.NewTimer(interval)
// 	// if !timer.Stop() {
// 	// 	debug(dTimer, "timerResetHelper: timer already stopped")
// 	// 	select {
// 	// 	case <-timer.C:
// 	// 	default:
// 	// 	}
// 	// } else {
// 	// 	debug(dTimer, "timerResetHelper: timer stopped")
// 	// }
// 	// timer.Reset(interval)
// 	// debug(dTimer, "timerResetHelper: timer reset")
// }

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

	// channels to replace timer
	heartbeatCh chan struct{}
	grantVoteCh chan struct{}
	winElectCh  chan struct{}
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
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
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

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	Success bool
	Term    int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		// Reply false if term < currentTerm
		debug(dVote, "S%v receive lower term from S%v, reject vote", rf.me, args.CandidateId)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		// If RPC request contains term T > currentTerm:
		// set currentTerm = T, convert to follower
		debug(dVote, "S%v receive higher term from S%v RequestVote, convert to follower", rf.me, args.CandidateId)
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = FOLLOWER
		rf.heartbeatCh <- struct{}{}
	}

	// If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		debug(dVote, "S%v vote for S%v", rf.me, args.CandidateId)
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.votedFor = args.CandidateId
		rf.grantVoteCh <- struct{}{}

		// timerResetHelper(&rf.electionTimer, randomElectionTimeout())
	} else {
		debug(dVote, "S%v reject vote for S%v", rf.me, args.CandidateId)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

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
	// timerResetHelper(&rf.electionTimer, randomElectionTimeout())

	reply.Success = true
	reply.Term = rf.currentTerm
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

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
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (2A)
		// Check if a leader election should be started.

		// select {
		// case <-rf.electionTimer.C:
		// 	debug(dTimer, "S%v election timer expired", rf.me)
		// 	rf.mu.Lock()
		// 	if rf.state == LEADER {
		// 		debug(dLeader, "S%v is leader, do nothing", rf.me)
		// 		rf.mu.Unlock()
		// 		continue
		// 	}
		// 	// start election
		// 	debug(dTimer, "S%v start election", rf.me)
		// 	rf.state = CANDIDATE
		// 	rf.currentTerm++
		// 	rf.votedFor = rf.me
		// 	timerResetHelper(&rf.electionTimer, randomElectionTimeout())
		// 	rf.mu.Unlock()
		// 	go rf.startElection()
		// }

		rf.mu.RLock()
		state := rf.state
		rf.mu.RUnlock()

		switch state {
		case FOLLOWER:
			debug(dInfo, "S%v ticking as FOLLOWER", rf.me)
			select {
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
			select {
			case <-rf.heartbeatCh:
			case <-time.After(HEARTBEAT_INTERVAL):
				go rf.heartbeat()
			}
		}
	}
}

func (rf *Raft) preElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = CANDIDATE
	rf.currentTerm++
	rf.votedFor = rf.me
}

func (rf *Raft) startElection() {
	args := RequestVoteArgs{}
	rf.mu.RLock()
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
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
			debug(dVote, "S%v send RequestVote to S%v", rf.me, i)
			startTime := time.Now()
			if ok := rf.sendRequestVote(i, &args, &reply); !ok {
				debug(dVote, "S%v send RequestVote to S%v failed", rf.me, i)
				voteCh <- false
				return
			}
			// debug(dVote, "S%v receive reply from S%v", rf.me, i)
			debug(dVote, "S%v receive reply from S%v after %vus", rf.me, i, time.Since(startTime).Microseconds())
			if reply.Term > args.Term {
				// If RPC response contains term T > currentTerm:
				// set currentTerm = T, convert to follower
				debug(dVote, "S%v receive higher term from S%v, convert to follower", rf.me, i)
				rf.mu.Lock()
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.state = FOLLOWER
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
			rf.mu.Unlock()
			rf.winElectCh <- struct{}{}
			go rf.heartbeat()
			return
		}
	}
}

func (rf *Raft) heartbeat() {
	debug(dLeader, "S%v start broadcasting heartbeat", rf.me)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			debug(dLeader, "S%v send heartbeat to S%v", rf.me, i)
			args := AppendEntriesArgs{LeaderId: rf.me}
			reply := AppendEntriesReply{}
			rf.mu.RLock()
			args.Term = rf.currentTerm
			rf.mu.RUnlock()

			go func() {
				if ok := rf.sendAppendEntries(i, &args, &reply); !ok {
					debug(dLeader, "S%v send heartbeat to S%v failed", rf.me, i)
					return
				}
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					debug(dLeader, "S%v receive higher term from S%v during heartbeat, convert to follower", rf.me, i)
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.state = FOLLOWER
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
			}()
		}(i)
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

	rf.heartbeatCh = make(chan struct{}, 1)
	rf.grantVoteCh = make(chan struct{}, 1)
	rf.winElectCh = make(chan struct{}, 1)
	// rf.electionTimer = *time.NewTimer(randomElectionTimeout())

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
