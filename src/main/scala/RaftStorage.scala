import java.io.File
import java.util

import Raft.rpc.thrift.LogEntry
import Util.CircularFifoQueue
import com.google.common.primitives.{Ints, Longs}
import com.twitter.scrooge.TArrayByteTransport
import org.apache.thrift.protocol.TBinaryProtocol
import org.rocksdb.{Options, RocksDB, WriteBatchWithIndex, WriteOptions}

import scala.collection.mutable.ArrayBuffer

class RaftStorage(path: String, raftServer: RaftServer) extends Storage {
  RocksDB.loadLibrary()
  private val storagePath = new File(path).getPath + "/"
  protected val options: Options = new Options().setCreateIfMissing(true)
  //https://github.com/facebook/rocksdb/wiki/RocksDB-FAQ
  options.prepareForBulkLoad()

  protected def createDB(path: String): RocksDB = {
    new File(storagePath + path).mkdirs()
    RocksDB.open(options, path)
  }

  protected val logsDB: RocksDB = createDB("log")
  protected val fieldDB: RocksDB = createDB("field")
  protected val tempDB: RocksDB = createDB("temp")
  protected val idDB: RocksDB = createDB("id")
  protected val writeOptions = new WriteOptions

  private var logLength_ : Long = 0L

  private val cacheSizeLimit: Int = 1024

  private val tArrayByteTransport = new TArrayByteTransport()
  private val iprot = new TBinaryProtocol(tArrayByteTransport)

  private implicit class logEntry2ByteArray(logEntry: LogEntry) {
    def toByteArray: Array[Byte] = {
      tArrayByteTransport.reset()
      logEntry.write(iprot)
      tArrayByteTransport.toByteArray
    }
  }

  private implicit class deserialize(array: Array[Byte]) {
    def toLogEntry: LogEntry = {
      tArrayByteTransport.setBytes(array)
      LogEntry.decode(iprot)
    }

    def toInt: Int = {
      if (array == null) 0 else Ints.fromByteArray(array)
    }

    def toLong: Long = {
      if (array == null) 0 else Longs.fromByteArray(array)
    }
  }

  private implicit class Long2ArrayByte(x: Long) {
    def toByteArray: Array[Byte] = {
      Longs.toByteArray(x)
    }
  }

  private implicit class Int2ArrayByte(x: Int) {
    def toByteArray: Array[Byte] = {
      Ints.toByteArray(x)
    }
  }

  private def initial(): CircularFifoQueue[LogEntry] = {
    import collection.JavaConverters._

    val keys = ArrayBuffer("term".getBytes, "votedFor".getBytes, "logLength".getBytes)
    val results = fieldDB.multiGet(keys.asJava)

    term = results.get(keys(0)).toLong
    votedFor = results.get(keys(1)).toInt
    logLength_ = results.get(keys(2)).toLong
    val length = Math.min(cacheSizeLimit, logLength)
    val begin = Math.max(0, logLength - cacheSizeLimit)
    val indices = new util.ArrayList[Array[Byte]](length.toInt)
    for (i <- begin until length + begin) {
      indices.add(i.toByteArray)
    }
    val res = logsDB.multiGet(indices).asScala
    val logs = Array.fill[LogEntry](length.toInt)(null)
    for (i <- res) {
      logs(i._1.toInt + begin.toInt) = i._2.toLogEntry
    }
    val cache = new CircularFifoQueue[LogEntry](cacheSizeLimit)
    cache ++= logs
    cache
  }

  protected val logCache: CircularFifoQueue[LogEntry] = initial()

  def logLength: Long = logLength_

  def log(i: Long): LogEntry = {
    if (i >= logLength - cacheSizeLimit) {
      logCache((i - logLength + cacheSizeLimit).toInt)
    } else {
      logsDB.get(i.toByteArray).toLogEntry
    }
  }

  def lastTerm: Long = {
    if (logLength > 0) {
      logCache.last.term
    } else {
      0
    }
  }

  override def votedFor_=(v: Int): Unit = {
    vote = v
    fieldDB.put("votedFor".getBytes, v.toLong.toByteArray)
  }

  override def currentTerm: Long = term

  override def currentTerm_=(term: Long): Unit = {
    this.term = term
    fieldDB.put("term".getBytes, term.toByteArray)
    votedFor = 0
  }

  private def truncate(begin: Long): Unit = {
    val batch = new WriteBatchWithIndex()
    if (logLength - begin >= logCache.size) {
      logCache.foreach(logEntry => batch.remove(logEntry.uid.toByteArray))
      for (i <- begin until logLength - logCache.size) {
        val uid = log(i).uid
        batch.remove(uid.toByteArray)
      }
    } else {
      for (i <- begin until logLength) {
        val uid = log(i).uid
        batch.remove(uid.toByteArray)
      }
    }
    idDB.write(writeOptions, batch)
  }

  def existLog(uid: Long): Boolean = {
    idDB.get(uid.toByteArray) == null
  }

  override def addLog(logEntry: LogEntry): Unit = {
    val length = logCache.size.toLong.toByteArray
    logCache += logEntry
    fieldDB.put("logLength".getBytes, length)
    logsDB.put(length, logEntry.toByteArray)
    logLength_ += 1
    idDB.put(logEntry.uid.toByteArray, new Array[Byte](1))
  }

  def appendLog(start: Long, logEntries: Seq[LogEntry]): Unit = {
    val begin = Util.binarySearch(start,
      Math.min(start + logEntries.length, logLength),
      i => log(i) == logEntries((i - start).toInt))
    truncate(begin)
    val bias = (begin - start).toInt
    val logBatch = new WriteBatchWithIndex()
    val idBatch = new WriteBatchWithIndex()
    for (i <- bias until logEntries.length) {
      logBatch.put((i + begin).toByteArray, logEntries(i).toByteArray)
      idBatch.put(logEntries(i).uid.toByteArray, new Array[Byte](1))
    }
    logsDB.write(writeOptions, logBatch)
    idDB.write(writeOptions, idBatch)
    logCache ++= logEntries
    fieldDB.put("logLength".getBytes, logCache.size.toLong.toByteArray)
  }


}
