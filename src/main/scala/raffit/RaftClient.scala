package raffit

import java.nio.ByteBuffer

import Raft.rpc.thrift.{CommandResponse, RaftService}
import com.twitter.finagle.Thrift
import com.twitter.util.{Future, Return, Throw}

class RaftClient(private var cluster: Seq[String]) {

  private def newClient(address: String): RaftService.MethodPerEndpoint = {
    Thrift.client.newIface[RaftService.MethodPerEndpoint](address)
  }

  private var raftLeaderId = 0
  private var client = newClient(cluster.head)

  def sendCommand[T](command: ByteBuffer, uid: Long): Future[T] = {
    client.sendCommand(command, uid).transform({
      case Throw(_) => sendCommand(command, uid)
      case Return(CommandResponse(leaderId)) =>
        if (leaderId < 0) {
          raftLeaderId = (1 + raftLeaderId) % cluster.size
        } else {
          raftLeaderId = leaderId
        }
        client = newClient(cluster(raftLeaderId))
        sendCommand(command, uid)
    })
  }
}
