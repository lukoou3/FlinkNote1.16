package com.flink.connector.jdbc

import java.sql.PreparedStatement
import org.apache.flink.configuration.Configuration
import com.flink.connector.common.BatchIntervalSink
import com.flink.utils.SingleValueMap

abstract class BatchIntervalJdbcSinkV2[T](
  connectionOptions: JdbcConnectionOptions,
  batchSize: Int,
  batchIntervalMs: Long,
  minPauseBetweenFlushMs: Long = 100L,
  keyedMode: Boolean = false,
  maxRetries: Int = 2,
  periodExecSqlStrategy: PeriodExecSqlStrategy = null,
  hasDelete: Boolean = false
) extends BatchIntervalSink[T](batchSize, batchIntervalMs, minPauseBetweenFlushMs, keyedMode) {
  self =>
  @transient var jdbcExecutorResource: SingleValueMap.ResourceData[JdbcBatchExecutor] = _
  @transient var batchStatement: JdbcBatchStatement[T] = _

  def updateSql: String

  def deleteSql: String = throw new Exception("hasDelete时必须实现")

  def isDeleteData(data: T): Boolean = throw new Exception("hasDelete时必须实现")

  def setStmt(stmt: PreparedStatement, data: T): Unit

  def setDeleteStmt(stmt: PreparedStatement, data: T): Unit = throw new Exception("hasDelete时必须实现")

  override def onInit(parameters: Configuration): Unit = {
    jdbcExecutorResource = SingleValueMap.acquireResourceData(connectionOptions, {
      val jdbcBatchExecutor = new JdbcBatchExecutor(connectionOptions)
      jdbcBatchExecutor.initConn()
      jdbcBatchExecutor
    })(_.close())
    batchStatement = new JdbcBatchStatement[T](jdbcExecutorResource.data, hasDelete, maxRetries, periodExecSqlStrategy) {
      override def setStmt(stmt: PreparedStatement, data: T): Unit = self.setStmt(stmt, data)
      override def setDeleteStmt(stmt: PreparedStatement, data: T): Unit = self.setDeleteStmt(stmt, data)
      override def isDeleteData(data: T): Boolean = self.isDeleteData(data)
      override def updateSql: String = self.updateSql
      override def deleteSql: String = self.deleteSql
    }
    jdbcExecutorResource.data.addBatchStatement(batchStatement)
  }

  override final def onFlush(datas: Iterable[T]): Unit = {
    batchStatement.flush(datas)
  }

  override final def onClose(): Unit = {
    logInfo("onClose ...")
    if(jdbcExecutorResource != null){
      jdbcExecutorResource.release()
    }
  }
}
