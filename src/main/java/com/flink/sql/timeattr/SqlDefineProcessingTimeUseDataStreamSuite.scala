package com.flink.sql.timeattr

import com.flink.base.FlinkBaseSuite
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._

/**
 * 和1.12一样，就是创建表时传入Expression*的方法已弃用，还有就是窗口时间toString显示的是当期时区的时间
 * DataStream-to-Table Conversion时定义proctime官网还是使用的弃用的方法fromDataStream[T](dataStream: DataStream[T], fields: Expression*)
 * 既然标记成弃用，官网还是用之前的代码真是蛋疼，而且中文的很多页面都是之前版本的页面，既然还没翻译直接引用英文的不行吗，真是服了flink
 * 看了下最新的1.15的, 还是使用的fromDataStream定义处理时间属性官网(测试1.14时的注释), 还是使用的fromDataStream定义处理时间属性
 * 现在1.6的官网
 *
 *
 * sql中定义处理时间的三种方式:
 *    https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/table/concepts/time_attributes/#processing-time
 *    https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/dev/table/concepts/time_attributes/#processing-time
 * 定义处理时间一般也就只会用到两种, 直接在最后一列额外声明一个处理时间字段即可, 形式是固定的:
 *    在创建表的 DDL 中定义, 格式如像: ts AS PROCTIME()
 *    在 DataStream 到 Table 转换时定义, 格式如像: 'ts.proctime
 */
class SqlDefineProcessingTimeUseDataStreamSuite extends FlinkBaseSuite {
  override def parallelism: Int = 2

  test("During DataStream-to-Table Conversion"){
    // 每个分区：每秒1个。整个应用：每秒2个，5秒10个
    val onlineLog: DataStream[OnlineLog] = env.addSource(new OnlineLogSouce(count = 1, sleepMillis = 1000, pageNum=1))
    println("Source parallelism:" + onlineLog.parallelism)

    // 声明一个额外的字段作为处理时间属性字段
    // 使用case类时，定义的列可以选择自定义顺序, 使用元组就不行
    // tEnv.createTemporaryView("tmp_tb", onlineLog, 'timeStr as 'ptime, 'pageId as 'page_id, 'visitCnt as 'cnt, 'ts.proctime)
    tEnv.createTemporaryView("tmp_tb",
      tEnv.fromDataStream(onlineLog,'timeStr as 'ptime, 'pageId as 'page_id, 'visitCnt as 'cnt, 'ts.proctime)
    )
    // 不行，proctime必须定义在table生成之前
    //tEnv.createTemporaryView("tmp_tb", tEnv.fromDataStream(onlineLog).select('timeStr as 'ptime, 'pageId as 'page_id, 'visitCnt as 'cnt, 'ts.proctime))

    val sql =
      """
    select
        TUMBLE_START(ts, INTERVAL '5' SECOND) as wstart,
        TUMBLE_END(ts, INTERVAL '5' SECOND) as wend,
        min(ptime) min_ptime,
        max(ptime) max_ptime,
        page_id,
        sum(cnt) pv
    from tmp_tb
    group by page_id, TUMBLE(ts, INTERVAL '5' SECOND)
    """
    val rstTable = tEnv.sqlQuery(sql)
    rstTable.printSchema()

    //rstTable.toAppendStream[Row].print()
    rstTable.execute().print()
  }

}
