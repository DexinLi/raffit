import java.util.concurrent.atomic.AtomicInteger

import Raft.rpc.thrift.{RequestVote, RequestVoteResponse}
import RaftServer.ServerState
import com.twitter.util.Throw

import scala.util.Random

class Candidate(raftServer: RaftServer) extends Thread {
  def sleepTime(): Int = Random.nextInt(150) + 150

  var timeout = false

  private def checkState: Boolean = {
    raftServer.stateReadLock.lock()
    val valid = raftServer.state == ServerState.Candidate
    raftServer.stateReadLock.unlock()
    valid
  }

  override def run(): Unit = {
    raftServer.receiveLock.synchronized {
      do {
        val votesNum = new AtomicInteger(1)
        raftServer.storageWriteLock.lock()
        raftServer.currentTerm += 1
        raftServer.storageWriteLock.unlock()
        val time = sleepTime()
        raftServer.receiveLock.wait(time)
        if (raftServer.logReceived) {
          //raftServer.becomeFollower()
          return
        }
        raftServer.storageReadLock.lock()
        val vote = if (raftServer.logLength > 0) {
          RequestVote(memberId = raftServer.memberId,
            term = raftServer.currentTerm,
            lastLogIndex = raftServer.logLength,
            lastLogTerm = raftServer.lastTerm)
        } else {
          RequestVote(memberId = raftServer.memberId, term = raftServer.currentTerm)
        }
        raftServer.storageReadLock.unlock()
        for (client <- raftServer.clients) {
          client.sendRequestVote(vote).onSuccess {
            response =>
              raftServer.stateWriteLock.lock()
              if (raftServer.state == ServerState.Candidate) {
                response match {
                  case RequestVoteResponse(_, true) =>
                    if (votesNum.incrementAndGet() > raftServer.serverNum / 2) {
                      raftServer.becomeLeader()
                      raftServer.receiveLock.notify()
                    }
                  case RequestVoteResponse(term, false) =>
                    if (term > raftServer.currentTerm) {
                      raftServer.becomeFollower()
                      raftServer.receiveLock.notify()
                    }
                }
              }
              raftServer.stateWriteLock.unlock()
          }
        }
        raftServer.receiveLock.wait(raftServer.electionTimeout - time)
      } while (checkState)
    }
  }
}
