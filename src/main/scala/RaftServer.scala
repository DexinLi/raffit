import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.concurrent.locks.ReentrantReadWriteLock.{ReadLock, WriteLock}

import Raft.rpc.thrift._
import RaftServer.ServerState
import Util.Boxed
import com.twitter.finagle.Thrift
import com.twitter.util.Future

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
        return response
      }

      Future(impl())
    }

    override def sendRequestVote(requestVote: RequestVote): Future[RequestVoteResponse] = {
      def impl: RequestVoteResponse = {
        //        stateLock.synchronized {
        //          if (state == ServerState.Leader)
        //            return RequestVoteResponse(currentTerm = currentTerm, granted = false)
        //        }
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
      def checkState: Boolean = {
        stateReadLock.lock()
        val valid = state == ServerState.Leader
        stateReadLock.unlock()
        valid
      }

      if (!checkState) {
        Future(CommandResponse(leaderId = leaderId))
      } else {
        val log = LogEntry(term = currentTerm, command = command, uid = uid)
        storage.addLog(log)
        for (i <- 0 until serverNum) {
          clients(i).sendAppendEntries(AppendEntries(term = currentTerm,
            leaderId = memberId,
            leaderCommit = commitIndex,
            prevLogIndex = logLength,
            prevLogTerm = lastTerm,
            entries = Seq(log)))
        }
        Future {
          val lock = Boxed(false)
          clientReplies.put(uid, lock)
          lock.synchronized {
            lock.wait()
          }
          if (lock) {
            CommandResponse(leaderId = leaderId, Some(ByteBuffer.wrap(Array())))
          } else {
            CommandResponse(leaderId = leaderId)
          }
        }
      }
    }

  }
  val server = Thrift.server.serveIface("localhost:9000", RaftServerImpl)

  val clientReplies = new ConcurrentHashMap[Long, Boxed[Boolean]]()

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
