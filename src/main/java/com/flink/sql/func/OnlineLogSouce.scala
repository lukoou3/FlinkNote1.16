package com.flink.sql.func

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import scala.util.Random

class OnlineLogSouce(val count: Int = 5, val sleepMillis: Long = 1000L, pageNum: Int = 5,
  lateness: Boolean = false, useOneData: Boolean = false) extends RichParallelSourceFunction[OnlineLog] {
  println(this)
  println("OnlineLogSouce:count:" + count)
  var running = true
  var data = OnlineLog()

  override def open(parameters: Configuration): Unit = {
    println(this)
    println("OnlineLogSouce:open")
  }

  override def run(ctx: SourceFunction.SourceContext[OnlineLog]): Unit = {
    println("OnlineLogSouce:run")
    val random = new Random()
    var i = 0
    val beginTime = System.currentTimeMillis() / 1000 * 1000
    val fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    while (running) {
      for (_ <- (1 to this.count)) {
        val pageId = random.nextInt(pageNum).toString
        val userId = random.nextInt(100).toString
        //val eventTime = beginTime + i * 500
        val eventTime = if(i % 2 == 0) {
          beginTime + i * 500
        } else {
          beginTime + i * 500 - 1000 * 60 * 60 * 24
        }
        val time = System.currentTimeMillis()
        //println("source_data", eventTime, fmt.format(new Date(eventTime)), time, fmt.format(new Date(time)))
        ctx.collect(
          getData(pageId, userId, eventTime, time, fmt.format(new Date(eventTime)), fmt.format(new Date(time)), 1)
        )

        // 模拟延时
        if (lateness && i % 5 == 0) {
          //println("888888888888888888888888")
          ctx.collect(
            getData(pageId, userId, eventTime - 1000 * 10, time, fmt.format(new Date(eventTime - 1000 * 10)), fmt.format(new Date(time)), 1)
          )
        }
      }


      i += 1
      // 模拟每隔一段时间产生几条浏览日志
      Thread.sleep(sleepMillis)
    }
  }

  def getData(pageId: String, userId: String, eventTime: Long,
    time: Long, eventTimeStr: String, timeStr: String, visitCnt: Int): OnlineLog = {
    if(useOneData){
      data.pageId = pageId
      data.userId = userId
      data.eventTime = eventTime
      data.time = time
      data.eventTimeStr = eventTimeStr
      data.timeStr = timeStr
      data.visitCnt = visitCnt
      data
    }else{
      OnlineLog(pageId, userId, eventTime, time, eventTimeStr, timeStr, visitCnt)
    }
  }

  override def cancel(): Unit = this.running = false
}
