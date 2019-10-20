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
	"log"
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

const (
	LEADER    = iota
	CANDIDATE = iota
	FOLLOWER  = iota
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	wg sync.WaitGroup

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//persistent state
	currentTerm int
	votedFor    int //default -1
	log         []Entry

	//volatile state on all servers
	commitIndex int
	lastApplied int
	status      int
	toFollower  chan int
	toCandidate chan int
	isLeader    chan int

	//volatile state on leaders
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm

	if rf.status == LEADER {
		isleader = true
		log.Println(rf.me, " is leader ")
	} else {
		isleader = false
	}

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//log.Println(rf.me, " Receive VoteRequest from: ", args.CandidateId)

	if rf.status == FOLLOWER {
		if args.Term < rf.currentTerm {
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			return
		}

		rf.toFollower <- args.Term
		reply.Term = args.Term
		if rf.votedFor == -1 {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
		} else if rf.votedFor == args.CandidateId {
			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
		}
	} else if args.Term > rf.currentTerm {
		rf.toFollower <- args.Term
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]Entry, 1)
	rf.status = FOLLOWER
	rf.toFollower = make(chan int, 1)
	rf.toCandidate = make(chan int, 1)
	rf.isLeader = make(chan int, 1)

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, 1)
	rf.matchIndex = make([]int, 1)

	log.Println("make raft", rf.me)

	go func() {
		for {
			randTime := rand.Intn(500)
			select {
			case term := <-rf.toFollower:
				if term >= rf.currentTerm {
					rf.currentTerm = term
					log.Println(rf.me, " is follower")
					rf.status = FOLLOWER
				}

			case term := <-rf.toCandidate:
				if term > rf.currentTerm {
					//reset votedFor
					rf.votedFor = -1
					rf.currentTerm = term
					rf.status = CANDIDATE
					log.Println(rf.me, " is candidate")
					rf.LaunchElection()
				}

			case term := <-rf.isLeader:
				if term < rf.currentTerm {
					break
				}
				rf.status = LEADER
				//reset votedFor
				rf.votedFor = -1
				//rf.isLeader <- rf.currentTerm
				log.Println(rf.me, " is leader!!!")
				for rf.DistributeAppendEntries() {

				}

			case <-time.After(time.Millisecond * (500 + time.Duration(randTime))):

				if rf.status == FOLLOWER || rf.status == CANDIDATE {
					rf.toCandidate <- rf.currentTerm + 1
				}
			}
		}
	}()

	// initialize from state persisted before a crash

	rf.readPersist(persister.ReadRaftState())

	return rf
}

type Entry struct {
	Command interface{}
	Term    int
}

func (rf *Raft) LaunchElection() {
	term := rf.currentTerm
	arg := RequestVoteArgs{term, rf.me, len(rf.log) - 1, rf.log[len(rf.log)-1].Term}
	rly := RequestVoteReply{}
	rf.votedFor = rf.me
	votes := 1

	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.wg.Add(1)
		go func(args RequestVoteArgs, reply RequestVoteReply, peer int) {
			defer rf.wg.Done()

			if rf.sendRequestVote(peer, &args, &reply) == false {
				//log.Printf("reply content %v /n", reply)
				return
			}

			if reply.VoteGranted == true {
				rf.mu.Lock()
				votes++
				rf.mu.Unlock()
			} else if reply.Term > term {
				term = reply.Term
			}

		}(arg, rly, i)
	}

	rf.wg.Wait()
	log.Println("candidate ", rf.me, " votes is ", votes, " all is ", len(rf.peers))
	if term > rf.currentTerm {
		rf.toFollower <- term
	} else if votes > len(rf.peers)/2 {
		rf.isLeader <- rf.currentTerm
		log.Println(rf.me, " is becoming leader")
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PreLogIndex  int
	PreLogTerm   int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//reset votedFor
	//log.Println(rf.me, " Receive AppendEntries from: ", args.LeaderId)
	rf.votedFor = -1
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
	} else if len(args.Entries) == 0 {
		rf.toFollower <- args.Term
		reply.Success = true
	} else if len(rf.log) <= args.PreLogIndex || rf.log[args.PreLogIndex].Term != args.PreLogTerm {
		reply.Success = false
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) DistributeAppendEntries() bool {
	term := rf.currentTerm
	replica := 1
	arg := AppendEntriesArgs{Term: term, LeaderId: rf.me, PreLogIndex: 0,
		PreLogTerm: 0, Entries: make([]Entry, 0), LeaderCommit: rf.commitIndex}
	rly := AppendEntriesReply{}

	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.wg.Add(1)
		go func(args AppendEntriesArgs, reply AppendEntriesReply, peer int) {
			defer rf.wg.Done()
			//if rf.sendAppendEntries(peer, &args, &reply) == false {
			//	return
			//}
			if rf.sendAppendEntries(peer, &args, &reply) == false {
				//log.Printf("reply content %v /n", reply)
				return
			}

			if reply.Term > term {
				term = reply.Term
			} else if reply.Success == true {
				rf.mu.Lock()
				replica++
				rf.mu.Unlock()
			}

		}(arg, rly, i)
	}

	rf.wg.Wait()
	if term > rf.currentTerm {
		rf.toFollower <- term
	} else if replica > len(rf.peers)/2 {
		return true
	}
	return false
}
