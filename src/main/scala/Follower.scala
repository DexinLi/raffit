import Raft.rpc.thrift.{AppendEntriesResponse, AppendEntries}
import RaftServer.ServerState
import com.twitter.util.Future

class Follower(raftServer: RaftServer) extends Thread {
  val electionTimeout = 500

  private def checkState: Boolean = {
    //raftServer.stateReadLock.lock()
    val valid = raftServer.state == ServerState.Follower
    //raftServer.stateReadLock.unlock()
    valid
  }

  override def run(): Unit = {
    raftServer.receiveLock.synchronized {
      do {
        raftServer.logReceived = false
        raftServer.receiveLock.wait(electionTimeout)
      } while (raftServer.logReceived)
      raftServer.becomeCandidate()
    }
  }
}
