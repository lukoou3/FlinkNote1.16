package com.flink.connector.jdbc

import com.flink.log.Logging

import java.sql.{Connection, PreparedStatement, SQLException}
import JdbcBatchStatement._

import java.io.IOException

abstract class JdbcBatchStatement[T](
  jdbcBatchExecutor:JdbcBatchExecutor,
  hasDelete: Boolean,
  maxRetries: Int,
  periodExecSqlStrategy: PeriodExecSqlStrategy
) extends Logging{
  val id = nextId
  var stmt: PreparedStatement = _
  var deleteStmt: PreparedStatement = _

  def updateSql: String

  def deleteSql: String

  def isDeleteData(data: T): Boolean

  def setStmt(stmt: PreparedStatement, data: T): Unit

  def setDeleteStmt(stmt: PreparedStatement, data: T): Unit

  def initStmt(conn: Connection): Unit = {
    if(stmt != null){
      throw new Exception("已经init")
    }
    stmt = conn.prepareStatement(updateSql)
    logWarning("gene_update_sql:" + updateSql)
    if(hasDelete){
      deleteStmt = conn.prepareStatement(deleteSql)
      logWarning("gene_delete_sql:" + deleteSql)
    }
  }

  def closeStmt(): Unit = {
    logInfo(s"close JdbcBatchStatement, updateSql:$updateSql")
    if (stmt != null) {
      logInfo("close stmt")
      stmt.close()
      stmt = null
    }
    if (deleteStmt != null) {
      logInfo("close deleteStmt")
      deleteStmt.close()
      deleteStmt = null
    }
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
            if (!jdbcBatchExecutor.isConnValid()) {
              jdbcBatchExecutor.closeConn()
              jdbcBatchExecutor.initConn()

              jdbcBatchExecutor.closeBatchStatements()
              jdbcBatchExecutor.initBatchStatements()
            }
          } catch {
            case e: Exception =>
              throw new IOException("Reestablish JDBC connection failed", e)
          }
      }

      i += 1
    }
  }

  final def flush(datas: Iterable[T]): Unit = jdbcBatchExecutor.synchronized {
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

  private def saveDatas(datas: Iterable[T]): Unit = {
    for (data <- datas) {
      //logWarning(data.toString)
      setStmt(stmt, data)
      stmt.addBatch()
    }
    stmt.executeBatch()
  }

  private def delDatas(datas: Iterable[T]): Unit = {
    for (data <- datas) {
      //logWarning(data.toString)
      setDeleteStmt(deleteStmt, data)
      deleteStmt.addBatch()
    }
    deleteStmt.executeBatch()
  }

  private def periodExecSql(): Unit = {
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

}

object JdbcBatchStatement{
  var id = 0

  def nextId: Int = synchronized {
    id = id + 1
    id
  }
}