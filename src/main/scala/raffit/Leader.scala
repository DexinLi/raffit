package raffit

import Raft.rpc.thrift.{AppendEntries, AppendEntriesResponse, LogEntry}
import com.twitter.util.{Future, Return, Throw}
import raffit.RaftServer.ServerState
import raffit.Util.Boxed

import scala.collection.mutable.ArrayBuffer

class Leader(raftServer: RaftServer) extends Thread {
  val heartbeatTime = 150
  protected val serverNum: Int = {
    raftServer.storageReadLock.lock()
    val num = raftServer.serverNum
    raftServer.storageReadLock.unlock()
    num
  }

  private def checkState: Boolean = {
    raftServer.stateReadLock.lock()
    val valid = raftServer.state == ServerState.Leader
    raftServer.stateReadLock.unlock()
    valid
  }

  val heartbeatLocks = ArrayBuffer.fill[Boxed[Boolean]](serverNum)(Boxed(false))
  val heartbeatThreads = new ArrayBuffer[Thread](serverNum)
  for (i <- 0 until serverNum) {
    heartbeatThreads(i) = new Thread(() => {
      heartbeatLocks(i).synchronized {
        while (checkState) {
          if (!heartbeatLocks(i)) {
            try {
              raftServer.storageReadLock.lock()
              val appendEntries = heartbeatAppendEntry
              sendAppendEntriesWithRetry(i, appendEntries)
            } finally {
              raftServer.storageReadLock.unlock()
            }
            heartbeatLocks(i).set(false)
          }
          heartbeatLocks(i).wait(heartbeatTime)
        }
      }
    })
  }

  protected val initialNextIndex: Long = {
    raftServer.storageReadLock.lock()
    val logLength = raftServer.logLength
    raftServer.storageReadLock.unlock()
    logLength
  }

  val nextIndex = ArrayBuffer.fill(serverNum)(initialNextIndex)
  val matchIndex = ArrayBuffer.fill(serverNum)(0L)

  private def clip(x: Long): Int = {
    val limit = 128
    Math.max(x, limit).toInt
  }

  def appendEntry(logEntry: LogEntry): Unit = {
    raftServer.storageWriteLock.lock()
    raftServer.storage.addLog(logEntry)
    val entry = AppendEntries(
      term = raftServer.currentTerm,
      leaderId = raftServer.memberId,
      leaderCommit = raftServer.commitIndex,
      prevLogIndex = raftServer.logLength - 1,
      prevLogTerm = raftServer.lastTerm,
      entries = Seq(logEntry))
    raftServer.storageWriteLock.unlock()
    for (i <- 0 until serverNum) {
      sendAppendEntriesWithRetry(i, entry)
    }
  }

  protected def appendMoreEntries(i: Int, lastTerm: Long): Future[Unit] = {
    val client = raftServer.clients(i)
    var res: Future[Unit] = Future()
    raftServer.stateReadLock.lock()
    if (raftServer.state == ServerState.Leader) {
      raftServer.storageReadLock.lock()
      val end = raftServer.logLength
      if (end > 0) {
        val begin = Util.binarySearch(0, end, raftServer.log(_).term < lastTerm)
        nextIndex(i) = begin
        val length = clip(raftServer.logLength - begin)
        val logEntries = new ArrayBuffer[LogEntry](length)
        for (i <- 0 until length) {
          logEntries(i) = raftServer.log(begin + i)
        }
        val appendEntry = AppendEntries(
          term = raftServer.currentTerm,
          leaderId = raftServer.leaderId,
          leaderCommit = raftServer.commitIndex,
          prevLogIndex = begin - 1,
          prevLogTerm = raftServer.log(begin).term,
          entries = logEntries)
        raftServer.storageReadLock.unlock()
        res = client.sendAppendEntries(appendEntry).transform({
          case Throw(_) => sendAppendEntriesWithRetry(i, appendEntry)
          case Return(AppendEntriesResponse(term, false, _)) =>
            raftServer.stateWriteLock.lock()
            raftServer.storageWriteLock.lock()
            val ct = raftServer.currentTerm
            val future = if (term > ct) {
              raftServer.currentTerm = term
              raftServer.becomeFollower()
              Future()
            } else {
              appendMoreEntries(i, raftServer.log(begin - 1).term)
            }
            raftServer.storageWriteLock.unlock()
            raftServer.stateWriteLock.unlock()
            future
          case Return(AppendEntriesResponse(_, true, index)) =>
            matchIndex(i) = index
            var future = Future()
            heartbeatLocks(i).synchronized {
              while (nextIndex(i) > matchIndex(i)) {
                raftServer.storageReadLock.lock()
                try {
                  val logs = for (i <- index + 1 until raftServer.logLength)
                    yield raftServer.log(i)
                  future = sendAppendEntriesWithRetry(i,
                    AppendEntries(
                      term = raftServer.currentTerm,
                      leaderId = raftServer.memberId,
                      leaderCommit = raftServer.commitIndex,
                      prevLogIndex = index,
                      prevLogTerm = raftServer.log(index).term,
                      entries = logs))
                } finally {
                  raftServer.storageReadLock.unlock()
                }
              }
            }
            future
        })
        heartbeatLocks(i).synchronized {
          heartbeatLocks(i).set(true)
          heartbeatLocks(i).notify()
        }
      } else {
        raftServer.storageReadLock.unlock()
      }
    }
    raftServer.stateReadLock.unlock()
    res
  }

  protected def sendAppendEntriesWithRetry(i: Int, appendEntries: AppendEntries): Future[Unit] = {
    val client = raftServer.clients(i)
    var res = Future()
    raftServer.stateReadLock.lock()
    if (raftServer.state == ServerState.Leader) {
      res = client.sendAppendEntries(appendEntries).transform({
        case Throw(_) => sendAppendEntriesWithRetry(i, appendEntries)
        case Return(AppendEntriesResponse(term, false, _)) =>
          raftServer.stateWriteLock.lock()
          raftServer.storageWriteLock.lock()
          val ct = raftServer.currentTerm
          if (term > ct) {
            raftServer.currentTerm = term
            raftServer.becomeFollower()
            raftServer.storageWriteLock.unlock()
            raftServer.stateWriteLock.unlock()
            Future()
          } else {
            appendMoreEntries(i, term)
          }
        case Return(AppendEntriesResponse(_, true, index)) =>
          matchIndex(i) = index
          var future = Future()
          heartbeatLocks(i).synchronized {
            while (nextIndex(i) > matchIndex(i)) {
              raftServer.storageReadLock.lock()
              try {
                val logs = for (i <- index + 1 until raftServer.logLength) yield raftServer.log(i)
                future = sendAppendEntriesWithRetry(i,
                  AppendEntries(
                    term = raftServer.currentTerm,
                    leaderId = raftServer.memberId,
                    leaderCommit = raftServer.commitIndex,
                    prevLogIndex = index,
                    prevLogTerm = raftServer.log(index).term,
                    entries = logs))
              } finally {
                raftServer.storageReadLock.unlock()
              }
            }
            future
          }
      })
      heartbeatLocks(i).synchronized {
        heartbeatLocks(i).set(true)
      }
      heartbeatLocks(i).notify()
    }
    raftServer.stateReadLock.unlock()
    res
  }

  def heartbeatAppendEntry: AppendEntries = AppendEntries(
    term = raftServer.currentTerm,
    leaderId = raftServer.memberId,
    leaderCommit = raftServer.commitIndex,
    prevLogIndex = raftServer.logLength - 1,
    prevLogTerm = raftServer.lastTerm)

  override def run(): Unit = {
    raftServer.leaderId = raftServer.memberId
    for (thread <- heartbeatThreads) {
      thread.start()
    }
  }
}
