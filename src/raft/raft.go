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
	//	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

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

// role type
const Follower = 0
const Candidate = 1
const Leader = 2

// 日志项
type LogEntry struct {
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm    int
	votedFor       int
	timestamp      time.Time     // count of ticks since last election
	timeLimit      time.Duration // time limit for election
	role           int           //Follower, Candidate, Leader
	voteCount      int           // count of votes received
	log            []LogEntry
	heartBeatLimit time.Duration // time limit for heart beat
	commitIndex    int
	lastApplied    int

	nextIndex  []int //for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int //for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term := rf.currentTerm
	isleader := false
	if rf.role == Leader {
		isleader = true
	}
	// Your code here (2A).
	return term, isleader
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
	LeaderId     int
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

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 如果请求的term小于当前term，直接拒绝
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	// 如果请求的term大于当前term，更新当前term 成为follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.role = Follower
	}
	// 如果当前节点没有投票，或者投票给了请求的节点，且请求的term大于当前term
	lastLogTerm := 0
	if len(rf.log) != 0 {
		lastLogTerm = rf.log[len(rf.log)-1].Term
	}
	// 如果请求的log term 大于当前节点的log term，或者term相同但是index更大
	// 这里是 >= ?
	// fmt.Printf("args.LastLogTerm %d args.LastLogIndex %d lastLogTerm %d LastLogIndex %d\n", args.LastLogTerm, args.LastLogIndex, lastLogTerm, len(rf.log))
	if (args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= len(rf.log))) && (rf.votedFor == -1 || rf.votedFor == args.LeaderId) {
		rf.votedFor = args.LeaderId
		reply.Term = args.Term
		reply.VoteGranted = true
		rf.role = Follower
		rf.timestamp = time.Now()
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
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

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PreLogIndex  int
	PreLogTerm   int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// 仅心跳包
	// 心跳包的term一定是leader的term
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false
	// 如果请求的term小于当前term，直接拒绝
	if args.Term < rf.currentTerm {
		fmt.Printf("node %d reject appendEntries from node %d, args.Term %d rf.currentTerm %d\n", rf.me, args.LeaderId, args.Term, rf.currentTerm)
		return
	}
	if len(rf.log) <= args.PreLogIndex {
		fmt.Printf("node %d reject appendEntries from node %d, loglen %d  PrelogIndex %d \n", rf.me, args.LeaderId, len(rf.log), args.PreLogIndex)
		return
	}

	if args.PreLogIndex > 0 && rf.log[args.PreLogIndex].Term != args.PreLogTerm {
		fmt.Printf("node %d reject appendEntries from node %d, log[args.PreLogIndex].Term %d args.PreLogTerm %d\n", rf.me, args.LeaderId, rf.log[args.PreLogIndex].Term, args.PreLogTerm)
		return
	}

	if args.Term == rf.currentTerm && rf.votedFor != args.LeaderId {
		rf.currentTerm = args.Term
		rf.votedFor = args.LeaderId
		rf.role = Follower
		fmt.Printf("node %d reject appendEntries from node %d, votedFor %d LeaderId %d\n", rf.me, args.LeaderId, rf.votedFor, args.LeaderId)
		return 
	}

	// if args.Term == rf.currentTerm && rf.votedFor == args.LeaderId {
		
	// }
	// 如果请求的term大于当前term，更新当前term 成为follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = args.LeaderId
		rf.role = Follower
		// reply.Term = args.Term
		// reply.Success = true
		// rf.timestamp = time.Now()
		// rf.timeLimit = time.Duration((200 + (rand.Int63() % 500))) * time.Millisecond
	}
	for i , entry := range args.Entries {
		if len(rf.log) > args.PreLogIndex + i + 1 {
			rf.log[args.PreLogIndex + i + 1] = entry
		}else{
			rf.log = append(rf.log, entry)
		}
		fmt.Printf("node %d append [%d]log %v\n", rf.me, args.PreLogIndex+i + 1, rf.log[args.PreLogIndex + i + 1].Command)
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.PreLogIndex + len(args.Entries) 
		fmt.Printf("node %d update commitIndex %d\n", rf.me, rf.commitIndex)
	}
	fmt.Printf("node %d accept appendEntries from node %d, loglen %d\n", rf.me, args.LeaderId, len(rf.log))
	reply.Term = rf.currentTerm
	reply.Success = true
	rf.timestamp = time.Now()
	rf.timeLimit = time.Duration((200 + (rand.Int63() % 500))) * time.Millisecond
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != Leader {
		return -1, -1, false
	}
	logEntry := LogEntry{
		Command: command,
		Term:    rf.currentTerm,
	}
	rf.log = append(rf.log, logEntry)
	index = len(rf.log)
	term = rf.currentTerm
	fmt.Printf("start: node %d is leader at term %d append log\n", rf.me, rf.currentTerm)
	// index 起始值为 1
	return index, term, isLeader
}

func (rf *Raft) applyLog(applyCh chan ApplyMsg) {

	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)
		var appliedMsgs = make([]ApplyMsg, 0)
		rf.mu.Lock()
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied+1, // 起始值为1
			}
			appliedMsgs = append(appliedMsgs, msg)
		}
		rf.mu.Unlock()
		for _, msg := range appliedMsgs {
			applyCh <- msg
		}
	}
	
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

func(rf *Raft) handleAppendEntriesReply(raft int,reply *AppendEntriesReply, args *AppendEntriesArgs) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.role = Follower
		rf.votedFor = -1
		return
	}

	if reply.Success {
		// 成功
		// 更新nextIndex
		
		rf.matchIndex[raft] = args.PreLogIndex + len(args.Entries)
		rf.nextIndex[raft] = rf.matchIndex[raft] + 1
		fmt.Printf("node %d append success to node %d nextIndex %d matchIndex %d\n", rf.me, raft, rf.nextIndex[raft], rf.matchIndex[raft])
		rf.updateCommitIndex()
	}else{
		// 失败
		// 减小nextIndex
		rf.nextIndex[raft] -= 1
		if rf.nextIndex[raft] < 0 {
			rf.nextIndex[raft] = 0
		}
	}
}


func (rf *Raft) updateCommitIndex() {
	// 数字N, 让nextIndex[i]的大多数>=N
	// peer[0]' index=2
	// peer[1]' index=2
	// peer[2]' index=1
	// 1,2,2
	// 更新commitIndex, 就是找中位数
	sortedMatchIndex := make([]int, 0)
	sortedMatchIndex = append(sortedMatchIndex, len(rf.log)-1)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		sortedMatchIndex = append(sortedMatchIndex, rf.matchIndex[i])
	}
	sort.Ints(sortedMatchIndex)
	fmt.Printf("node %d sortedMatchIndex %v\n", rf.me, sortedMatchIndex)
	newCommitIndex := sortedMatchIndex[len(rf.peers)/2]
	// 如果index属于snapshot范围，那么不要检查term了，因为snapshot的一定是集群提交的
	// 否则还是检查log的term是否满足条件
	if newCommitIndex > rf.commitIndex && (rf.log[newCommitIndex].Term == rf.currentTerm) {
		rf.commitIndex = newCommitIndex
	}
	fmt.Printf("LeaderNode[%d] updateCommitIndex, commitIndex[%d] \n", rf.me, rf.commitIndex)
}

func (rf *Raft) ticker() {

	for rf.killed() == false {
		// 每10ms 唤醒一次进行检查
		time.Sleep(time.Duration(10) * time.Millisecond)
		// Your code here (2A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if time.Since(rf.timestamp) > rf.timeLimit && rf.role != Leader {
			// leader 是不需要参与选举的
			// start a leader election
			// send RequestVote RPCs to all other servers
			// 成为candidate
			rf.role = Candidate
			// 重置时间戳
			rf.timestamp = time.Now()
			rf.timeLimit = time.Duration((500 + (rand.Int63() % 500))) * time.Millisecond
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.voteCount = 1
			// 构造投票请求
			args := RequestVoteArgs{
				Term:     rf.currentTerm,
				LeaderId: rf.me,
				LastLogIndex: len(rf.log),
				LastLogTerm: 0,
			}
			if len(rf.log) != 0 {
				args.LastLogTerm = rf.log[len(rf.log)-1].Term
			}
			// 向其他节点发送投票请求
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				go func(server int) {
					reply := RequestVoteReply{}
					// 在从节点调用 RequestVote
					rf.sendRequestVote(server, &args, &reply)
					// 处理投票结果
					rf.handleRequestVoteReply(&reply)
				}(i)
			}
		}
		if rf.role == Leader && time.Since(rf.timestamp) > rf.heartBeatLimit {

			// leader 发送心跳包
			
			for i := 0; i < len(rf.peers); i++ {
				if rf.role != Leader {
					break
				}
				if i == rf.me {
					continue
				}

				args := AppendEntriesArgs{
					Term:     rf.currentTerm,
					LeaderId: rf.me,
					LeaderCommit: rf.commitIndex,
					Entries: make([]LogEntry, 0),
					PreLogIndex: rf.nextIndex[i]-1,
					PreLogTerm: 0,
				}
				if args.PreLogIndex >= 0 && args.PreLogIndex < len(rf.log){
					args.PreLogTerm = rf.log[args.PreLogIndex].Term
				}
				// 从nextIndex[i]开始发送
				if rf.nextIndex[i] < len(rf.log) {
					
					args.Entries = append(args.Entries, rf.log[rf.nextIndex[i]:]...)
				}
				fmt.Printf("leader %d term %d send heart beat to node %d entires num %d Prelogindex %d commiteIndex %d\n", rf.me, rf.currentTerm,i, len(args.Entries), args.PreLogIndex, args.LeaderCommit)
				// log 相关

				go func(server int) {
					reply := AppendEntriesReply{}
					rf.sendAppendEntries(server, &args, &reply)
					rf.handleAppendEntriesReply(server, &reply, &args)
				}(i)
			}
			// 重置时间戳
			rf.timestamp = time.Now()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) handleRequestVoteReply(reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.role = Follower
		rf.votedFor = -1
		return
	}

	if reply.VoteGranted && rf.role == Candidate && reply.Term == rf.currentTerm {
		// 收到投票
		rf.voteCount++
		if rf.voteCount > len(rf.peers)/2 {
			rf.role = Leader
			// 初始化leader的数据
			rf.timestamp = time.Now()
			rf.votedFor = rf.me
			rf.nextIndex = make([]int, len(rf.peers))
			rf.matchIndex = make([]int, len(rf.peers))
			for i:=0; i<len(rf.peers); i++ {
				rf.nextIndex[i] = len(rf.log) + 1
				rf.matchIndex[i] = -1
			}
			// fmt.Printf("node %d is leader at term %d \n", rf.me, rf.currentTerm)
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
	rf.currentTerm = 0
	// 创建一个初始的时钟
	rf.timestamp = time.Now()
	rf.timeLimit = time.Duration((50 + (rand.Int63() % 300))) * time.Millisecond
	rf.heartBeatLimit = time.Duration(50) * time.Millisecond
	rf.role = Follower
	rf.voteCount = 0
	rf.lastApplied = -1
	rf.commitIndex = -1
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)

	
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyLog(applyCh)
	return rf
}
