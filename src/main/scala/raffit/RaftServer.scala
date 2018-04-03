package raffit

import java.nio.ByteBuffer
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.concurrent.locks.ReentrantReadWriteLock.{ReadLock, WriteLock}

import Raft.rpc.thrift._
import com.twitter.finagle.Thrift
import com.twitter.util.Future
import raffit.RaftServer.ServerState

object RaftServer {

  object ServerState extends Enumeration {
    type ServerState = Value
    val Leader, Follower, Candidate = Value
  }

}

class RaftServer(cluster: Seq[String], val memberId: Int, dbPath: String) {

  def this(cluster: Seq[String], memberId: Int) =
    this(cluster, memberId, "db/" + memberId.toString)

  type Client = RaftService.MethodPerEndpoint

  val storageReadWriteLock = new ReentrantReadWriteLock()

  def storageReadLock: ReadLock = storageReadWriteLock.readLock()

  def storageWriteLock: WriteLock = storageReadWriteLock.writeLock()

  val storage = new RaftStorage(dbPath, this)

  def currentTerm: Long = storage.currentTerm

  def currentTerm_=(term: Long): Unit = {
    storage.currentTerm = term
  }

  val electionTimeout = 500

  var commitIndex: Long = -1
  var lastApplied: Long = -1

  var leaderId: Int = -1
  var serverNum: Int = cluster.length - 1

  var state = ServerState.Follower

  val stateReadWriteLock = new ReentrantReadWriteLock()

  val stateReadLock: ReadLock = stateReadWriteLock.readLock()

  val stateWriteLock: WriteLock = stateReadWriteLock.writeLock()

  val receiveLock = new AnyRef

  val clients: Seq[Client] = for (replica <- cluster.tail)
    yield Thrift.client.newIface[Client](replica)

  object RaftServerImpl extends RaftService.MethodPerEndpoint {
    override def sendAppendEntries(appendEntries: AppendEntries): Future[AppendEntriesResponse] = {
      def receive(): Unit = {
        receiveLock.synchronized {
          logReceived = true
          receiveLock.notify()
        }
      }

      def impl(): AppendEntriesResponse = {
        storageReadLock.lock()
        if (appendEntries.term < currentTerm) {
          val response = AppendEntriesResponse(term = currentTerm, success = false)
          storageReadLock.unlock()
          return response
        }
        leaderId = appendEntries.leaderId
        if (appendEntries.prevLogIndex < 0) {
          val response = AppendEntriesResponse(term = currentTerm, success = true)
          storageReadLock.unlock()
          receive()
          return response
        }
        if (appendEntries.prevLogIndex > logLength - 1) {
          val response = AppendEntriesResponse(term = currentTerm, success = false)
          storageReadLock.unlock()
          receive()
          return response
        }
        if (appendEntries.prevLogTerm != log(appendEntries.prevLogIndex).term) {
          val response = AppendEntriesResponse(term = currentTerm, success = false)
          storageReadLock.unlock()
          receive()
          return response
        }
        storageReadLock.unlock()
        val response = receiveLock.synchronized {
          storageWriteLock.lock()
          if (appendEntries.leaderCommit > commitIndex) {
            commitIndex = Math.min(appendEntries.leaderCommit, logLength)
          }
          storage.appendLog(appendEntries.prevLogIndex + 1, appendEntries.entries)
          val appendEntriesResponse = AppendEntriesResponse(term = currentTerm,
            success = true,
            matchIndex = logLength - 1)
          if (appendEntries.term > currentTerm) {
            currentTerm = appendEntries.term
            becomeFollower()
          }
          storageWriteLock.unlock()
          logReceived = true
          receiveLock.notify()
          appendEntriesResponse
        }
        response
      }

      Future(impl())
    }

    override def sendRequestVote(requestVote: RequestVote): Future[RequestVoteResponse] = {
      def impl: RequestVoteResponse = {
        storageWriteLock.lock()
        val response = if (currentTerm > requestVote.term) {
          RequestVoteResponse(currentTerm = currentTerm, granted = false)
        } else if (lastTerm > requestVote.lastLogTerm) {
          RequestVoteResponse(currentTerm = currentTerm, granted = false)
        } else if (lastTerm == requestVote.lastLogTerm && logLength > requestVote.lastLogIndex + 1) {
          RequestVoteResponse(currentTerm = currentTerm, granted = false)
        } else {
          if (currentTerm < requestVote.term) {
            currentTerm = requestVote.term
            votedFor(requestVote.memberId)
            becomeFollower()
            RequestVoteResponse(currentTerm = requestVote.term, granted = true)
          } else {
            RequestVoteResponse(currentTerm = requestVote.term, granted = isVoted)
          }
        }
        storageWriteLock.unlock()
        response
      }

      Future(impl)
    }

    override def sendCommand(command: ByteBuffer, uid: Long): Future[CommandResponse] = {
      stateReadLock.lock()
      val ret = if (state != ServerState.Leader) {
        Future(CommandResponse(leaderId = leaderId))
      } else {
        val log = LogEntry(term = currentTerm, command = command, uid = uid)
        Future {
          leaderThread.appendEntry(log)
          CommandResponse(leaderId)
        }
      }
      stateReadLock.unlock()
      ret
    }

  }

  val server = Thrift.server.serveIface(cluster.head, RaftServerImpl)

  def log(i: Long): LogEntry = storage.log(i)

  def logLength: Long = storage.logLength

  def lastTerm: Long = storage.lastTerm

  def isVoted: Boolean = storage.votedFor > 0

  def votedFor(id: Int): Unit = {
    storage.votedFor = id
  }

  var logReceived = false

  val followerThread = new Follower(this)
  val leaderThread = new Leader(this)
  val candidateThread = new Candidate(this)

  def becomeFollower(): Unit = {
    if (state != ServerState.Follower) {
      state = ServerState.Follower
      leaderId = -1
      followerThread.start()
    }
  }

  def becomeCandidate(): Unit = {
    if (state != ServerState.Candidate) {
      state = ServerState.Candidate
      leaderId = -1
      candidateThread.start()
    }
  }

  def becomeLeader(): Unit = {
    if (state != ServerState.Leader) {
      state = ServerState.Leader
      leaderThread.start()
    }
  }
}
