package com.flink.sql.idlestate

import java.time.Duration

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, _}
import org.apache.flink.table.api.bridge.scala._

/**
 * https://www.jianshu.com/p/1880bfa2539c
 * https://www.jianshu.com/p/9ae1d2974304
 * flink sql中不要忘记忘记设置空闲状态保留时间, 默认是0, 状态永远不会清理
 * idle state retention time特性可以保证当状态中某个key对应的数据未更新的时间达到阈值时，该条状态被自动清理。
 * 设置的方式：tEnv.getConfig().setIdleStateRetention(Duration.ofMinutes(1))
 * 之前的设置方式已经标记为弃用, setIdleStateRetentionTime(Time minTime, Time maxTime), tEnv.getConfig().setIdleStateRetentionTime(Time.hours(24), Time.hours(36))
 * 现在setIdleStateRetentionTime的代码中就只是校验maxTime - minTime不能小于5分钟, 然后把minTime传入: setIdleStateRetention(Duration duration)
 *
 * sql的配置似乎只能在代码和提交参数中配置, 官网中没发现sql set配置的语法, ：
 *    sql配置: https://ci.apache.org/projects/flink/flink-docs-release-1.12/zh/dev/table/config.html
 *    sql语法: https://ci.apache.org/projects/flink/flink-docs-release-1.12/zh/dev/table/sql/
 *
  a b
  a b
  c d
  cd
  c d
  c d
  a b
 */
object IdleStateRetentionTimeTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    val settings = EnvironmentSettings.newInstance().inStreamingMode().build()
    val tEnv = StreamTableEnvironment.create(env, settings)

    // 默认是0, 永远不会清理
    tEnv.getConfig().setIdleStateRetention(Duration.ofMinutes(1))

    // 需要先启动`nc -lk 9999`，用来发送数据，windows使用`nc -l -p 9999`命令
    val text = env.socketTextStream("localhost", 9999)

    val words: DataStream[Tuple1[String]] = text.flatMap(_.split("\\s+").map(_.trim).filter(_.nonEmpty)).map(Tuple1(_))

    tEnv.createTemporaryView("tmp_tb", words, 'word)

    val sql =
      """
    select
        word,
        current_timestamp `current_timestamp`,
        count(1) cnt
    from tmp_tb
    group by word
    """
    val rstTable = tEnv.sqlQuery(sql)

    //rstTable.toAppendStream[Row].print()
    rstTable.execute().print()

    env.execute()
  }

}
