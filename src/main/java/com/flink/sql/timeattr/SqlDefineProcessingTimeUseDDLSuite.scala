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
class SqlDefineProcessingTimeUseDDLSuite extends FlinkBaseSuite {
  override def parallelism: Int = 2

  test("Defining in DDL"){
    // 每个分区：每秒1个。整个应用：每秒2个，5秒10个
    val createTbSql =
      """
CREATE TABLE tmp_tb (
  page_id int,
  cnt int,
  proc_time AS PROCTIME() -- 声明一个额外的列作为处理时间属性
) WITH (
  'connector'='datagen',
  'rows-per-second'='1',
  'fields.page_id.min'='0',
  'fields.page_id.max'='0',
  'fields.cnt.min'='1',
  'fields.cnt.max'='1'
)
    """
    tEnv.executeSql(createTbSql)

    val sql =
      """
    select
        TUMBLE_START(proc_time, INTERVAL '5' SECOND) as wstart,
        TUMBLE_END(proc_time, INTERVAL '5' SECOND) as wend,
        min(proc_time) min_proc_time,
        max(proc_time) max_proc_time,
        page_id,
        sum(cnt) pv
    from tmp_tb
    group by page_id, TUMBLE(proc_time, INTERVAL '5' SECOND)
    """

    val rstTable = tEnv.sqlQuery(sql)
    tEnv.sqlQuery("select * from tmp_tb").printSchema()
    rstTable.printSchema()

    //rstTable.toAppendStream[Row].print()
    rstTable.execute().print()
  }

}
