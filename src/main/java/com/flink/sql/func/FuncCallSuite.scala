package com.flink.sql.func

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Table, _}
import org.apache.flink.types.Row
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
 * 内置函数:
 *  https://nightlies.apache.org/flink/flink-docs-release-1.12/zh/dev/table/functions/systemFunctions.html
 *  https://nightlies.apache.org/flink/flink-docs-release-1.14/zh/docs/dev/table/functions/systemfunctions/
 *  https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/dev/table/functions/systemfunctions/
 */
class FuncCallSuite extends AnyFunSuite with BeforeAndAfterAll{
  var env: StreamExecutionEnvironment = _
  var tEnv: StreamTableEnvironment = _
  var table: Table = _

  test("func call"){
    /**
     * 代码调式：
     *    'eventTimeStr.substring(1, 10) 映射到的是 [[org.apache.flink.table.functions.BuiltInFunctionDefinitions.SUBSTRING]]
     *    BuiltInFunctionDefinitions 定义很多内置的BuiltInFunctionDefinition
     *    BuiltInFunctionDefinition会映射到SqlOperator，映射的map定义在[[org.apache.flink.table.planner.expressions.converter.DirectConvertRule]]
     *    在DirectConvertRule的convert方法中打断点能看到转换的时机
     *    代码生成转化在：[[org.apache.flink.table.planner.codegen.calls.StringCallGen.generateCallExpression]]
     *    可以看到: case SUBSTR | SUBSTRING => generateSubString(ctx, operands, returnType)
     *    generateSubString方法中可以看到生成的代码调用的是[[org.apache.flink.table.data.binary.BinaryStringDataUtil.substringSQL]]
     *
     * 总结一下：
     *    sql函数转换看：[[org.apache.flink.table.planner.codegen.calls.StringCallGen.generateCallExpression]]
     *    里面可以看到实际生成调用的代码，这里substring生成的代码调用的是: [[org.apache.flink.table.data.binary.BinaryStringDataUtil.substringSQL]]
     *
     */
    val rstTable: Table = table.select(
      'pageId,
      'userId,
      'eventTimeStr,
      'eventTimeStr.substring(1, 10)
    )

    rstTable.toAppendStream[Row].addSink(new SinkFunction[Row] {
      override def invoke(row: Row, context: SinkFunction.Context): Unit = {
        println(row)
      }
    })

  }

  test("func call times"){
    tEnv.createTemporaryView("tmp_tb", table)

    /**
     * [[org.apache.flink.table.planner.codegen.calls.StringCallGen.generateCallExpression]]
     * [[org.apache.flink.table.utils.DateTimeUtils.unixTimestamp()]]
     *
     * [[org.apache.flink.table.planner.codegen.calls.CurrentTimePointCallGen.generate()]]
     * [[org.apache.flink.table.planner.expressions.CurrentTimestamp]]
     */
    val sql =
      """
    select
        pageId,
        userId,
        eventTimeStr,
        timeStr,
        `time`,
        UNIX_TIMESTAMP() unix_ts,
        current_timestamp ts,
        LOCALTIMESTAMP local_ts,
        cast(current_timestamp as string) ts2,
        cast(LOCALTIMESTAMP as string) local_ts2,
        current_date `date`,
        substr(eventTimeStr, 1, 10) d2
    from tmp_tb
    where substr(eventTimeStr, 1, 10) = current_date
      and timeStr <= LOCALTIMESTAMP
    """
    val rstTable = tEnv.sqlQuery(sql)

    rstTable.execute().print()
    /*rstTable.toAppendStream[Row].addSink(new SinkFunction[Row] {
      override def invoke(row: Row, context: SinkFunction.Context): Unit = {
        println(row)
      }
    })*/
  }

  test("null"){
    tEnv.createTemporaryView("tmp_tb", table)

    /**
     *
     */
    val sql =
      """
    select
        pageId,
        userId,
        eventTimeStr,
        timeStr,
        `time`,
        substr(timeStr, 1, 10) d,
        substr(nullField, 1, 10) d_na,
        regexp(timeStr, '[0-9]{4}-[0-9]{2}-[0-9]{2}') re,
        regexp(nullField, '[0-9]{4}-[0-9]{2}-[0-9]{2}') d_re
    from tmp_tb
    """
    val rstTable = tEnv.sqlQuery(sql)

    rstTable.execute().print()
  }

  override protected def beforeAll(): Unit = {
    val conf = new Configuration()
    env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    // 每秒2个，5秒10个
    env.setParallelism(1)
    // ChainingOutput, 默认是CopyingChainingOutput
    env.getConfig.enableObjectReuse()

    val settings = EnvironmentSettings.newInstance().inStreamingMode().build()
    tEnv = StreamTableEnvironment.create(env, settings)

    // 每个分区：每秒1个。整个应用：每秒2个，5秒10个
    val onlineLog: DataStream[OnlineLog] = env.addSource(new OnlineLogSouce(count = 1, sleepMillis = 1000, pageNum = 1, useOneData = true))
    println("Source parallelism:" + onlineLog.parallelism)

    table = tEnv.fromDataStream(onlineLog)
  }

  override protected def afterAll(): Unit = {
    env.execute()
  }
}
