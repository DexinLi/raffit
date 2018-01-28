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

  def sendCommand[T](command: ByteBuffer, uid: Long, handler: ByteBuffer => T): Future[T] = {
    client.sendCommand(command, uid).transform({
      case Throw(_) => sendCommand(command, uid, handler)
      case Return(CommandResponse(leaderId, None)) =>
        if (leaderId < 0) {
          raftLeaderId = (1 + raftLeaderId) % cluster.size
        } else {
          raftLeaderId = leaderId
        }
        client = newClient(cluster(raftLeaderId))
        sendCommand(command, uid, handler)
      case Return(CommandResponse(_, Some(buffer))) =>
        Future(handler(buffer))
    })
  }
}
