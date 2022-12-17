package com.flink.connector.jdbc

import com.flink.log.Logging

import java.sql.{Connection, DriverManager, SQLException}
import scala.collection.mutable

class JdbcBatchExecutor(connectionOptions: JdbcConnectionOptions) extends Logging{
  var conn: Connection = _
  val batchStatements = new mutable.HashMap[Int, JdbcBatchStatement[_]]()

  def initConn(): Unit = {
    val JdbcConnectionOptions(url, username, password, driverName) = connectionOptions
    Class.forName(driverName)
    conn = DriverManager.getConnection(url, username, password)
    conn.setAutoCommit(true)
    logInfo("open conn")
  }

  def closeConn(): Unit = {
    if (conn != null) {
      logInfo("close conn")
      try {
        conn.close()
      } catch {
        case e: SQLException => logWarning("JDBC connection close failed.", e)
      }finally{
        conn = null
      }
    }
  }

  def isConnValid(): Boolean = conn.isValid(60)

  def addBatchStatement[T](batchStatement: JdbcBatchStatement[T]): Unit = synchronized {
    if(batchStatements.contains(batchStatement.id)){
      throw new Exception("重复添加batchStatement")
    }
    batchStatement.initStmt(conn)
    batchStatements.put(batchStatement.id, batchStatement)
  }

  def initBatchStatements(): Unit = {
    for((_, batchStat) <- batchStatements) {
      batchStat.initStmt(conn)
    }
  }

  def closeBatchStatements(): Unit = {
    for((_, batchStat) <- batchStatements) {
      batchStat.closeStmt()
    }
  }

  def close(): Unit = {
    closeBatchStatements()
    closeConn()
  }
}

