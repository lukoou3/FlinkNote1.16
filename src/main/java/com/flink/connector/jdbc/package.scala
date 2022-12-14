package com.flink.connector

import java.sql.PreparedStatement
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.catalog.ResolvedSchema
import org.apache.flink.table.data.RowData
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo
import org.apache.flink.table.types.logical.LogicalType
import org.apache.flink.table.types.logical.LogicalTypeRoot.{BIGINT, CHAR, DOUBLE, FLOAT, INTEGER, VARCHAR}

import scala.collection.JavaConverters._
import com.flink.connector.common.Utils
import com.flink.log.Logging
import com.flink.utils.TsUtils.{DAY_UNIT, HOUR_UNIT, hourOfDay, minuteOfHour, weekDay}
import org.apache.flink.streaming.api.functions.sink.SinkFunction

package object jdbc extends Logging{

  //JdbcSinkOptions
  //JdbcConnectionOptions
  case class JdbcConnectionOptions(
    url: String,
    username: String,
    password: String,
    driverName: String
  )

  trait PeriodExecSqlStrategy extends Serializable {
    def sqlsThisTime(ts: Long): List[String]
  }

  // 每日执行一次sql, 默认凌晨0点0分左右
  class PeriodExecSqlStrategyOncePerDay(hour:Int = 0, minute:Int = 0)(getSqls: Long => List[String]) extends PeriodExecSqlStrategy{
    var lastExecSqlTs: Long = 0L
    override def sqlsThisTime(ts: Long): List[String] = {
      if (ts - lastExecSqlTs >= HOUR_UNIT * 11 && hourOfDay(ts) == hour && minuteOfHour(ts) >= minute) {
        lastExecSqlTs = ts
        getSqls(ts)
      }else{
        Nil
      }
    }
  }

  // 每周执行一次sql, 默认周一凌晨0点0分左右
  class PeriodExecSqlStrategyOncePerWeek(week:Int = 0, hour:Int = 0, minute:Int = 0)(getSqls: Long => List[String]) extends PeriodExecSqlStrategy{
    var lastExecSqlTs: Long = 0L
    override def sqlsThisTime(ts: Long): List[String] = {
      if (ts - lastExecSqlTs >= DAY_UNIT * 6 && weekDay(ts) == week
        && hourOfDay(ts) == hour && minuteOfHour(ts) >= minute) {
        lastExecSqlTs = ts
        getSqls(ts)
      }else{
        Nil
      }
    }
  }

  implicit class ProductDataStreamJdbcFunctions[T <: Product : TypeInformation](ds: DataStream[T]) {
    def addBatchIntervalJdbcSink(
      tableName: String,
      connectionOptions: JdbcConnectionOptions,
      batchSize: Int,
      batchIntervalMs: Long,
      minPauseBetweenFlushMs: Long = 100L,
      maxRetries: Int = 2,
      fieldsForTuple: Seq[String] = Nil,
      isUpdateMode: Boolean = true,
      oldValcols: Seq[String] = Nil,
      fieldColMap: Map[String, String] = Map.empty[String, String],
      objectReuse: Boolean = false,
      connReuse: Boolean = false
    ): DataStreamSink[T] = {
      // 提醒作用, 调用者显示设置objectReuse=true则认为他知道objectReuse的影响
      assert(ds.executionEnvironment.getConfig.isObjectReuseEnabled == objectReuse)
      val productTypeInformation = implicitly[TypeInformation[T]].asInstanceOf[CaseClassTypeInfo[T]]
      val fieldNames = if (fieldsForTuple.isEmpty) {
        productTypeInformation.fieldNames
      } else {
        assert(fieldsForTuple.length == productTypeInformation.fieldNames.length)
        fieldsForTuple
      }
      val cols = fieldNames.map(x => fieldColMap.getOrElse(x, x))
      val names = fieldNames //fieldNames.zipWithIndex

      val sql = geneFlinkJdbcSql(tableName, cols, oldValcols, isUpdateMode)
      logWarning("gene_sql:" + sql)
      println("gene_sql:" + sql)

      val sink = if(!connReuse){
        new BatchIntervalJdbcSink[T](connectionOptions, batchSize = batchSize, batchIntervalMs = batchIntervalMs,
          minPauseBetweenFlushMs = minPauseBetweenFlushMs, maxRetries = maxRetries) {
          val numFields: Int = names.length

          override def updateSql: String = sql

          override def setStmt(stmt: PreparedStatement, data: T): Unit = {
            var i = 0
            while (i < numFields) {
              val v: AnyRef = data.productElement(i) match {
                case null | None => null
                case x: java.lang.Integer => x
                case x: java.lang.Long => x
                case x: java.lang.Double => x
                case x: java.sql.Timestamp => x
                case x: String => x
                case Some(s: AnyRef) => s match {
                  case null | None => null
                  case x: java.lang.Integer => x
                  case x: java.lang.Long => x
                  case x: java.lang.Double => x
                  case x: java.sql.Timestamp => x
                  case x: String => x
                  case x => throw new UnsupportedOperationException(s"unsupported data $x")
                }
                case x => throw new UnsupportedOperationException(s"unsupported data $x")
              }
              stmt.setObject(i + 1, v)
              i = i + 1
            }
          }

        }
      }else{
        new BatchIntervalJdbcSinkV2[T](connectionOptions, batchSize = batchSize, batchIntervalMs = batchIntervalMs,
          minPauseBetweenFlushMs = minPauseBetweenFlushMs, maxRetries = maxRetries) {
          val numFields: Int = names.length

          override def updateSql: String = sql

          override def setStmt(stmt: PreparedStatement, data: T): Unit = {
            var i = 0
            while (i < numFields) {
              val v: AnyRef = data.productElement(i) match {
                case null | None => null
                case x: java.lang.Integer => x
                case x: java.lang.Long => x
                case x: java.lang.Double => x
                case x: java.sql.Timestamp => x
                case x: String => x
                case Some(s: AnyRef) => s match {
                  case null | None => null
                  case x: java.lang.Integer => x
                  case x: java.lang.Long => x
                  case x: java.lang.Double => x
                  case x: java.sql.Timestamp => x
                  case x: String => x
                  case x => throw new UnsupportedOperationException(s"unsupported data $x")
                }
                case x => throw new UnsupportedOperationException(s"unsupported data $x")
              }
              stmt.setObject(i + 1, v)
              i = i + 1
            }
          }

        }
      }

      ds.addSink(sink).name(s"jdbc-$tableName")
    }
  }

  implicit class DataStreamJdbcFunctions[T: TypeInformation](ds: DataStream[T]) {
    def addKeyedBatchIntervalJdbcSink[K, E <: Product : TypeInformation](keyFunc: T => K)(dateFunc: T => E)(
      tableName: String,
      connectionOptions: JdbcConnectionOptions,
      batchSize: Int,
      batchIntervalMs: Long,
      minPauseBetweenFlushMs: Long = 100L,
      replaceDaTaValue: (T, T) => T = (newValue: T, oldValue: T) => newValue,
      maxRetries: Int = 2,
      fieldsForTuple: Seq[String] = Nil,
      isUpdateMode: Boolean = true,
      fieldMappingExclude: Seq[String] = Nil,
      oldValcols: Seq[String] = Nil,
      fieldColMap: Map[String, String] = Map.empty[String, String],
      hasDelete: Boolean = false,
      deleteKeyFields: Seq[String] = Nil,
      isDelete: T => Boolean = null,
      objectReuse: Boolean = false,
      connReuse: Boolean = false
    ): DataStreamSink[T] = {
      // 提醒作用, 调用者显示设置objectReuse=true则认为他知道objectReuse的影响
      assert(ds.executionEnvironment.getConfig.isObjectReuseEnabled == objectReuse)
      val productTypeInformation = implicitly[TypeInformation[E]].asInstanceOf[CaseClassTypeInfo[E]]
      val fieldNames = if (fieldsForTuple.isEmpty) {
        productTypeInformation.fieldNames
      } else {
        assert(fieldsForTuple.length == productTypeInformation.fieldNames.length)
        fieldsForTuple
      }

      // 为了实现简单，排除的属性必须定义在最后面
      if(fieldMappingExclude.nonEmpty){
        assert(fieldNames.endsWith(fieldMappingExclude), "排除的属性必须定义在最后面")
      }

      if(hasDelete){
        assert(isUpdateMode, "删除必须是UpdateMode")
        assert(deleteKeyFields.nonEmpty, "删除必须deleteKeyFields.nonEmpty")
        assert(isDelete != null, "删除必须isDelete != null")
      }

      val colFieldNames = fieldNames.filter(!fieldMappingExclude.contains(_))
      val cols = colFieldNames.map(x => fieldColMap.getOrElse(x, x))
      val names = colFieldNames //colFieldNames.zipWithIndex

      val sql = geneFlinkJdbcSql(tableName, cols, oldValcols, isUpdateMode)

      val delIdx = fieldNames.zipWithIndex.filter(x => deleteKeyFields.contains(x._1)).map(_._2)
      assert(deleteKeyFields.length == delIdx.length, "deleteKeyFields存在未知的属性")
      val delCols = deleteKeyFields.map(x => fieldColMap.getOrElse(x, x))
      val delWhere = delCols.map(col => s"$col = ?").mkString(" and ")
      val delSql = s"delete from $tableName where $delWhere"

      val sink = if(!connReuse){
        new BatchIntervalJdbcSink[T](connectionOptions, batchSize = batchSize, batchIntervalMs = batchIntervalMs,
          minPauseBetweenFlushMs = minPauseBetweenFlushMs, keyedMode = true, maxRetries = maxRetries, hasDelete = hasDelete) {
          val numFields: Int = names.length
          val delInfos = delIdx.zipWithIndex

          override def updateSql: String = sql

          override def getKey(data: T): Any = keyFunc(data)

          override def deleteSql: String = delSql

          override def isDeleteData(data: T): Boolean = isDelete(data)

          override def replaceValue(newValue: T, oldValue: T): T = replaceDaTaValue(newValue, oldValue)

          override def setStmt(stmt: PreparedStatement, row: T): Unit = {
            val data = dateFunc(row)
            var i = 0
            while (i < numFields) {
              val v: AnyRef = value2Jdbc(data.productElement(i))
              stmt.setObject(i + 1, v)
              i = i + 1
            }
          }

          override def setDeleteStmt(stmt: PreparedStatement, row: T): Unit = {
            val data = dateFunc(row)
            for ((elePos, i) <- delInfos) {
              val v: AnyRef = value2Jdbc(data.productElement(elePos))
              stmt.setObject(i + 1, v)
            }
          }

          def value2Jdbc(value: Any): AnyRef = value match {
            case null | None => null
            case x: java.lang.Integer => x
            case x: java.lang.Long => x
            case x: java.lang.Double => x
            case x: java.sql.Timestamp => x
            case x: String => x
            case Some(s: AnyRef) => s match {
              case null | None => null
              case x: java.lang.Integer => x
              case x: java.lang.Long => x
              case x: java.lang.Double => x
              case x: java.sql.Timestamp => x
              case x: String => x
              case x => throw new UnsupportedOperationException(s"unsupported data $x")
            }
            case x => throw new UnsupportedOperationException(s"unsupported data $x")
          }

        }
      }else{
        new BatchIntervalJdbcSinkV2[T](connectionOptions, batchSize = batchSize, batchIntervalMs = batchIntervalMs,
          minPauseBetweenFlushMs = minPauseBetweenFlushMs, keyedMode = true, maxRetries = maxRetries, hasDelete = hasDelete) {
          val numFields: Int = names.length
          val delInfos = delIdx.zipWithIndex

          override def updateSql: String = sql

          override def getKey(data: T): Any = keyFunc(data)

          override def deleteSql: String = delSql

          override def isDeleteData(data: T): Boolean = isDelete(data)

          override def replaceValue(newValue: T, oldValue: T): T = replaceDaTaValue(newValue, oldValue)

          override def setStmt(stmt: PreparedStatement, row: T): Unit = {
            val data = dateFunc(row)
            var i = 0
            while (i < numFields) {
              val v: AnyRef = value2Jdbc(data.productElement(i))
              stmt.setObject(i + 1, v)
              i = i + 1
            }
          }

          override def setDeleteStmt(stmt: PreparedStatement, row: T): Unit = {
            val data = dateFunc(row)
            for ((elePos, i) <- delInfos) {
              val v: AnyRef = value2Jdbc(data.productElement(elePos))
              stmt.setObject(i + 1, v)
            }
          }

          def value2Jdbc(value: Any): AnyRef = value match {
            case null | None => null
            case x: java.lang.Integer => x
            case x: java.lang.Long => x
            case x: java.lang.Double => x
            case x: java.sql.Timestamp => x
            case x: String => x
            case Some(s: AnyRef) => s match {
              case null | None => null
              case x: java.lang.Integer => x
              case x: java.lang.Long => x
              case x: java.lang.Double => x
              case x: java.sql.Timestamp => x
              case x: String => x
              case x => throw new UnsupportedOperationException(s"unsupported data $x")
            }
            case x => throw new UnsupportedOperationException(s"unsupported data $x")
          }

        }
      }

      ds.addSink(sink).name(s"jdbc-$tableName")

    }
  }

  implicit class TableFunctions(table: Table) {
    def addRowDataBatchIntervalJdbcSink(
      params: JdbcSinkParams
    ): DataStreamSink[RowData] = {
      val sink = getRowDataBatchIntervalJdbcSink(table.getResolvedSchema, params)
      val rowDataDs = table.toDataStream[RowData](table.getResolvedSchema.toSourceRowDataType.bridgedTo(classOf[RowData]))
      rowDataDs.addSink(sink)
    }
  }

  case class JdbcSinkParams(
    tableName: String,
    connectionOptions: JdbcConnectionOptions,
    batchSize: Int,
    batchIntervalMs: Long,
    minPauseBetweenFlushMs: Long = 100L,
    keyedMode: Boolean = false,
    keys: Seq[String] = Nil,
    orderBy: Seq[(String, Boolean)] = Nil,
    maxRetries: Int = 2,
    isUpdateMode: Boolean = true,
    oldValcols: Seq[String] = Nil,
    periodExecSqlStrategy: PeriodExecSqlStrategy = null,
    connReuse: Boolean = false
  )


  def getRowDataBatchIntervalJdbcSink(
    resolvedSchema: ResolvedSchema,
    params: JdbcSinkParams
  ): SinkFunction[RowData] = {
    val typeInformation: InternalTypeInfo[RowData] = InternalTypeInfo.of(resolvedSchema.toSourceRowDataType.getLogicalType)
    val fieldInfos = resolvedSchema.getColumns.asScala.map(col => (col.getName, col.getDataType)).toArray
    val cols = fieldInfos.map(_._1)
    val sql = geneFlinkJdbcSql(params.tableName, cols, params.oldValcols, params.isUpdateMode)
    val _setters = fieldInfos.map { case (_, dataType) => makeSetter(dataType.getLogicalType) }
    val _getKey = Utils.getTableKeyFunction(resolvedSchema,params.keyedMode,params.keys)
    val tableOrdering = Utils.getTableOrdering(resolvedSchema, params.orderBy)

    if(!params.connReuse){
      new BatchIntervalJdbcSink[RowData](params.connectionOptions, params.batchSize, params.batchIntervalMs, params.minPauseBetweenFlushMs,
        params.keyedMode, params.maxRetries, params.periodExecSqlStrategy) {
        val setters = _setters
        val numFields = setters.length
        @transient var serializer: TypeSerializer[RowData] = _
        @transient var objectReuse = false

        def updateSql: String = sql

        override def onInit(parameters: Configuration): Unit = {
          super.onInit(parameters)
          objectReuse = getRuntimeContext.getExecutionConfig.isObjectReuseEnabled
          if (objectReuse) {
            serializer = typeInformation.createSerializer(getRuntimeContext.getExecutionConfig)
          }
        }

        override def valueTransform(data: RowData): RowData = {
          if (objectReuse) serializer.copy(data) else data
        }

        override def getKey(data: RowData): Any = _getKey(data)

        override def replaceValue(newValue: RowData, oldValue: RowData): RowData = if (!this.keyedMode) {
          super.replaceValue(newValue, oldValue)
        } else {
          if (tableOrdering.gteq(newValue, oldValue)) {
            newValue
          } else {
            oldValue
          }
        }

        def setStmt(stmt: PreparedStatement, row: RowData): Unit = {
          var i = 0
          while (i < numFields) {
            if (row.isNullAt(i)) {
              stmt.setObject(i + 1, null)
            } else {
              setters(i).apply(stmt, row, i)
            }
            i += 1
          }
        }
      }
    }else{
      new BatchIntervalJdbcSinkV2[RowData](params.connectionOptions, params.batchSize, params.batchIntervalMs, params.minPauseBetweenFlushMs,
        params.keyedMode, params.maxRetries, params.periodExecSqlStrategy) {
        val setters = _setters
        val numFields = setters.length
        @transient var serializer: TypeSerializer[RowData] = _
        @transient var objectReuse = false

        def updateSql: String = sql

        override def onInit(parameters: Configuration): Unit = {
          super.onInit(parameters)
          objectReuse = getRuntimeContext.getExecutionConfig.isObjectReuseEnabled
          if (objectReuse) {
            serializer = typeInformation.createSerializer(getRuntimeContext.getExecutionConfig)
          }
        }

        override def valueTransform(data: RowData): RowData = {
          if (objectReuse) serializer.copy(data) else data
        }

        override def getKey(data: RowData): Any = _getKey(data)

        override def replaceValue(newValue: RowData, oldValue: RowData): RowData = if (!this.keyedMode) {
          super.replaceValue(newValue, oldValue)
        } else {
          if (tableOrdering.gteq(newValue, oldValue)) {
            newValue
          } else {
            oldValue
          }
        }

        def setStmt(stmt: PreparedStatement, row: RowData): Unit = {
          var i = 0
          while (i < numFields) {
            if (row.isNullAt(i)) {
              stmt.setObject(i + 1, null)
            } else {
              setters(i).apply(stmt, row, i)
            }
            i += 1
          }
        }
      }
    }
  }


  private[jdbc] def geneFlinkJdbcSql(tableName: String, cols: Seq[String], oldValcols: Seq[String], isUpdateMode: Boolean): String = {
    val columns = cols.mkString(",")
    val placeholders = cols.map(_ => "?").mkString(",")
    val sql = if (isUpdateMode) {
      val duplicateSetting = cols.map { name =>
        if (oldValcols.contains(name)) {
          s"${name}=IFNULL(${name},VALUES(${name}))"
        } else {
          s"${name}=VALUES(${name})"
        }
      }.mkString(",")

      s"INSERT INTO $tableName ($columns) VALUES ($placeholders) ON DUPLICATE KEY UPDATE $duplicateSetting"
    } else {
      s"INSERT INTO $tableName ($columns) VALUES ($placeholders)"
    }
    sql
  }

  def makeSetter(logicalType: LogicalType):(PreparedStatement, RowData, Int) => Unit = logicalType.getTypeRoot match {
    case CHAR | VARCHAR => (stmt, row, i) =>
      stmt.setString(i + 1, row.getString(i).toString)
    case INTEGER => (stmt, row, i) =>
      stmt.setInt(i + 1, row.getInt(i))
    case BIGINT => (stmt, row, i) =>
      stmt.setLong(i + 1, row.getLong(i))
    case FLOAT => (stmt, row, i) =>
      stmt.setFloat(i + 1, row.getFloat(i))
    case DOUBLE => (stmt, row, i) =>
      stmt.setDouble(i + 1, row.getDouble(i))
    case _ => throw new UnsupportedOperationException(s"unsupported data type $logicalType")
  }
}
