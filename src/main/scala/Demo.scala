import java.io.File
import java.net.InetSocketAddress

import com.twitter.finagle.{Http, Service, Thrift, http}
import com.twitter.finagle.builder.ServerBuilder
import com.twitter.finagle.http.{Request, Response}
import com.twitter.scrooge.TArrayByteTransport
import com.twitter.util.Future
import org.apache.thrift.protocol.TBinaryProtocol
import org.rocksdb.{Options, RocksDB}

import scala.collection.mutable.ArrayBuffer

object Demo {

  object test {
    private val singleton: Unit = println("here")

    def initial() = singleton
  }

  class test {
    test.initial()
  }

  def main(args: Array[String]): Unit = {
    var cluster = for (i <- 0 until 5) yield "localhost:" + i
    val buff = ArrayBuffer[Int]()
    val t = new test
    val t1 = new test
    //val client =
    //      new Test.FinagledClient(ClientBuilder()
    //          .hosts("localhost:9000")
    //          .stack(Thrift.client)
    //          .hostConnectionLimit(1)
    //          .build(), RichClientParam())


    //    var t = AppendEntriesResponse(1L, true)
    //    val buffer = new TArrayByteTransport(512)
    //    val b = new TBinaryProtocol(buffer)
    //    t.write(b)
    //    var arr = buffer.toByteArray
    //    buffer.reset()
    //    t.write(b)
    //    arr = buffer.toByteArray
    //    buffer.setBytes(arr)
    //    t = AppendEntriesResponse.decode(new TBinaryProtocol(buffer))
    //    RocksDB.loadLibrary()
    //    val options = new Options().setCreateIfMissing(true)
    //    val db = RocksDB.open(options,"db")
    //    db.put("x".getBytes,"x".getBytes)
    //    db.put("x".getBytes,"y".getBytes)
    //    var res = new String(db.get("x".getBytes))

//        Thrift.client.newIface[Test.MethodPerEndpoint]("localhost:9000")
//        object MyImpl extends Test.MethodPerEndpoint {
//          override def testThrift(message: String): Future[String] = Future("hello, " + message)
//        }

//            val server = ServerBuilder()
//                .stack(Thrift.server)
//                .name("test_service")
//                .bindTo(new InetSocketAddress(9000))
//                .build(new Test.FinagledService(MyImpl, new TBinaryProtocol.Factory()))

        //val server = Thrift.server.serveIface("localhost:9000", MyImpl)
    println("start")
    val x = Array(1)
    //        def start() = {
    //          val raftServers = new Array[raffit.RaftServer](cluster.length)
    //          for (i <- cluster.indices) {
    //            cluster = cluster.tail :+ cluster.head
    //            raftServers(i) = new raffit.RaftServer(cluster, i+1)
    //          }
    //        }
  }

}

