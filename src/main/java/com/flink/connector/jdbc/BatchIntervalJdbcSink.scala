package com.flink.connector.jdbc

import java.io.IOException
import java.sql.{Connection, DriverManager, PreparedStatement, SQLException}

import org.apache.flink.configuration.Configuration

import scala.collection.Iterable
import com.flink.connector.common.BatchIntervalSink
import com.flink.log.Logging

abstract class BatchIntervalJdbcSink[T](
  connectionOptions: JdbcConnectionOptions,
  batchSize: Int,
  batchIntervalMs: Long,
  minPauseBetweenFlushMs: Long = 100L,
  keyedMode: Boolean = false,
  maxRetries: Int = 2,
  periodExecSqlStrategy: PeriodExecSqlStrategy = null,
  hasDelete: Boolean = false
) extends BatchIntervalSink[T](batchSize, batchIntervalMs, minPauseBetweenFlushMs, keyedMode) {
  @transient var conn: Connection = _
  @transient var stmt: PreparedStatement = _
  @transient var deleteStmt: PreparedStatement = _

  def updateSql: String

  def deleteSql: String = throw new Exception("hasDelete时必须实现")

  def isDeleteData(data: T): Boolean = throw new Exception("hasDelete时必须实现")

  def setStmt(stmt: PreparedStatement, data: T): Unit

  def setDeleteStmt(stmt: PreparedStatement, data: T): Unit = throw new Exception("hasDelete时必须实现")

  override def onInit(parameters: Configuration): Unit = {
    initConn()
    stmt = conn.prepareStatement(updateSql)
    logWarning("gene_update_sql:" + updateSql)
    if(hasDelete){
      deleteStmt = conn.prepareStatement(deleteSql)
      logWarning("gene_delete_sql:" + deleteSql)
    }
  }

  final def initConn(): Unit = {
    val JdbcConnectionOptions(url, username, password, driverName) = connectionOptions
    Class.forName(driverName)
    conn = DriverManager.getConnection(url, username, password)
    conn.setAutoCommit(true)
    logInfo("open conn")
    logInfo(s"sql:$updateSql")
  }

  private def jdbcOperateExec(func: => Unit): Unit = {
    var i = 0
    while (i <= maxRetries) {
      logDebug("Retriy num:" + i)
      try {
        // 成功直接返回
        func
        return
      } catch {
        case e: SQLException =>
          if (i >= maxRetries) {
            throw new IOException(e)
          }

          // 连接超时, 重新连接
          try {
            if (!conn.isValid(60)) {
              initConn()
              if (stmt != null) {
                stmt.close()
              }
              stmt = conn.prepareStatement(updateSql)
              if(hasDelete){
                if (deleteStmt != null) {
                  deleteStmt.close()
                }
                deleteStmt = conn.prepareStatement(deleteSql)
              }
            }
          } catch {
            case e: Exception =>
              throw new IOException("Reestablish JDBC connection failed", e)
          }
      }

      i += 1
    }
  }

  override final def onFlush(datas: Iterable[T]): Unit = {
    if (periodExecSqlStrategy != null) {
      periodExecSql()
    }
    logDebug("onFlush start")

    var datasSave:Iterable[T]  = null
    var datasDelete:Iterable[T]  = null
    if(!hasDelete){
      datasSave = datas
    }else{
      val datasTuple = datas.partition(!isDeleteData(_))
      datasSave = datasTuple._1
      datasDelete = datasTuple._2
    }

    if(datasSave.nonEmpty){
      logInfo(s"saveDatas:$datasSave")
      jdbcOperateExec(saveDatas(datasSave))
    }
    if(hasDelete && datasDelete.nonEmpty){
      logInfo(s"delDatas:$datasDelete")
      jdbcOperateExec(delDatas(datasDelete))
    }
  }

  final def saveDatas(datas: Iterable[T]): Unit = {
    for (data <- datas) {
      //logWarning(data.toString)
      setStmt(stmt, data)
      stmt.addBatch()
    }
    stmt.executeBatch()
  }

  final def delDatas(datas: Iterable[T]): Unit = {
    for (data <- datas) {
      //logWarning(data.toString)
      setDeleteStmt(deleteStmt, data)
      deleteStmt.addBatch()
    }
    deleteStmt.executeBatch()
  }

  final def periodExecSql(): Unit = {
    val ts = System.currentTimeMillis()
    val sqls = periodExecSqlStrategy.sqlsThisTime(ts)
    if (sqls.nonEmpty) {
      try {
        for (sql <- sqls) {
          val rst = stmt.executeUpdate(sql)
          logInfo(s"executeUpdate sql:$sql, rst:$rst")
        }
      } catch {
        case e: Exception =>
          logError("periodExecSql error", e)
      }
    }
  }

  override final def onClose(): Unit = {
    if (stmt != null) {
      logInfo("close stmt")
      stmt.close()
    }
    if (deleteStmt != null) {
      logInfo("close deleteStmt")
      deleteStmt.close()
    }
    if (conn != null) {
      logInfo("close conn")
      conn.close()
    }
  }
}
