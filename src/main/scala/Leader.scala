import Raft.rpc.thrift.{AppendEntriesResponse, AppendEntries, LogEntry}
import RaftServer.ServerState
import com.twitter.util.{Future, Return, Throw}

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

  val heartbeatLocks = ArrayBuffer.fill[Integer](serverNum)(0)
  val heartbeatThreads = new ArrayBuffer[Thread](serverNum)
  for (i <- 0 until serverNum) {
    heartbeatThreads(i) = new Thread(() => {
      heartbeatLocks(i).synchronized {
        while (checkState) {
          if (heartbeatLocks(i) == 0) {
            try {
              raftServer.storageReadLock.lock()
              sendAppendEntriesWithRetry(i, heartbeatAppendEntry)
            } finally {
              raftServer.storageReadLock.unlock()
            }
            heartbeatLocks(i) = 1
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

  def appendMoreEntries(i: Int, lastTerm: Long): Future[Unit] = {
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
        val logEntries = new Array[LogEntry](length)
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
            future
          case Return(AppendEntriesResponse(_, true, index)) =>
            matchIndex(i) = index
            heartbeatLocks(i).synchronized {
              if (nextIndex(i) > matchIndex(i)) {
                raftServer.storageReadLock.lock()
                try {
                  val logs = for (i <- index + 1 until raftServer.logLength)
                    yield raftServer.log(i)
                  sendAppendEntriesWithRetry(i,
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
              } else {
                Future()
              }
            }
        })
        heartbeatLocks(i).synchronized {
          heartbeatLocks(i) = 1
          heartbeatLocks(i).notify()
        }
      } else {
        raftServer.storageReadLock.unlock()
      }
    }
    raftServer.stateReadLock.unlock()
    res
  }

  def sendAppendEntriesWithRetry(i: Int, appendEntries: AppendEntries): Future[Unit] = {
    val client = raftServer.clients(i)
    var res = Future()
    raftServer.stateWriteLock.lock()
    if (raftServer.state == ServerState.Leader) {
      res = client.sendAppendEntries(appendEntries).transform({
        case Throw(_) => sendAppendEntriesWithRetry(i, appendEntries)
        case Return(AppendEntriesResponse(term, false, _)) =>
          raftServer.storageWriteLock.lock()
          val ct = raftServer.currentTerm
          if (term > ct) {
            raftServer.currentTerm = term
            raftServer.becomeFollower()
            raftServer.storageWriteLock.unlock()
            Future()
          } else {
            appendMoreEntries(i, term)
          }
        case Return(AppendEntriesResponse(_, true, index)) =>
          matchIndex(i) = index
          heartbeatLocks(i).synchronized {
            if (nextIndex(i) > matchIndex(i)) {
              raftServer.storageReadLock.lock()
              try {
                val logs = for (i <- index + 1 until raftServer.logLength) yield raftServer.log(i)
                sendAppendEntriesWithRetry(i,
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
            } else {
              Future()
            }
          }

      })
      heartbeatLocks(i).synchronized {
        heartbeatLocks(i) = 1
      }
      heartbeatLocks(i).notify()
    }
    raftServer.stateWriteLock.unlock()
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
