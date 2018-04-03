namespace java Raft.rpc.thrift

struct LogEntry {
	1: required i64 term;
	2: required binary command;
	3: required i64 UID;
}

struct AppendEntries {
	1: required i64 term;
	2: required i32 leaderId;
	3: optional i64 leaderCommit = -1;
	4: optional i64 prevLogIndex = -1;
	5: optional i64 prevLogTerm = -1;
	6: required list<LogEntry> entries;
}

struct AppendEntriesResponse {
	1: required i64 term;
	2: required bool success;
	3: optional i64 matchIndex = -1;
}

struct RequestVote {
	1: required i32 memberId;
	2: required i64 term;
	3: optional i64 lastLogIndex = -1;
	4: optional i64 lastLogTerm = -1;
}

struct RequestVoteResponse {
	1: required i64 currentTerm;
	2: required bool granted;
}

struct Snapshot {
	1: required binary stateMachineState;
	2: required i64 lastLogEntryIndex;
	3: required i64 lastLogEntryTerm;
}

struct InstallSnapshot {
	1: required i64 term;
	2: required i64 leaderId;
	3: required Snapshot snapshot;
}

struct InstallSnapshotResponse {
	1: required bool success;
}

struct JoinMember {
	1: required i32 memberId;
}

struct JoinMemberResponse {
	1: required bool success;
}

struct LeaderResponse {
  1: required binary response;
}

struct CommandResponse {
  1: required i32 leaderId;
//  2: optional binary response;
}

service RaftService {

	RequestVoteResponse sendRequestVote(1:RequestVote requestVote);

	AppendEntriesResponse sendAppendEntries(1:AppendEntries appendEntries);

	CommandResponse sendCommand(1:binary command, 2:i64 uid);

//	InstallSnapshotResponse sendInstallSnapshot(1:InstallSnapshot installSnapshot);
//
//	JoinMemberResponse sendJoinMember(1:JoinMember memberId);

}