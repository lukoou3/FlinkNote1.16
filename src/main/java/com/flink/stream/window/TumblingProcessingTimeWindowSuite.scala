package com.flink.stream.window

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.bridge.scala._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class TumblingProcessingTimeWindowSuite extends AnyFunSuite with BeforeAndAfterAll{
  var env: StreamExecutionEnvironment = _
  var tEnv: StreamTableEnvironment = _

  override protected def beforeAll(): Unit = {
    val conf = new Configuration()
    env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)
    env.getConfig.enableObjectReuse()

    // Flink 1.14后，旧的planner被移除了，默认就是BlinkPlanner
    //val settings = EnvironmentSettings.newInstance().inStreamingMode().build()
    //tEnv = StreamTableEnvironment.create(env, settings)
    // 其实直接这样就行
    tEnv = StreamTableEnvironment.create(env)
  }

  test("TumblingProcessingTimeWindow"){
    // 每秒2个，5秒10个
    env.setParallelism(2)

    val onlineLog: DataStream[OnlineLog] = env.addSource(new OnlineLogSouce(1, 1000, 1))
    println("Source parallelism:" + onlineLog.parallelism)

    /* text.flatMap {
       line =>
         val array = line.split("\t")
         if (array.size < 4) {
           None
         } else {
           val pageId = array(0)
           val userId = array(1)
           val eventTime = array(2).toLong
           val time = array(3).toLong
           Some((pageId, userId, eventTime, time, 1))
         }
     }.keyBy(_._1)
       .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
       .sum(4)*/
    // 滚动窗口：每5秒计算个页面的pv
    onlineLog
      .keyBy(_.pageId)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .sum("visitCnt")
      .print()

  }


  override protected def afterAll(): Unit = {
    env.execute()
  }
}
