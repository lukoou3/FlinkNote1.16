package com.flink.stream.window

import com.flink.base.FlinkBaseSuite
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.bridge.scala._


class TumblingProcessingTimeWindowSuite extends FlinkBaseSuite{
  override def parallelism: Int = 1

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

}
