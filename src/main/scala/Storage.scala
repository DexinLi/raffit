import Raft.rpc.thrift.LogEntry

import scala.collection.mutable.ArrayBuffer

trait Storage {
  protected var term: Long = 0L
  protected var vote: Int = 0

  def votedFor: Int = vote

  def votedFor_=(v: Int): Unit

  def currentTerm: Long = term

  def currentTerm_=(term: Long): Unit

  def addLog(logEntry: LogEntry): Unit
}