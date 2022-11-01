package com.flink.stream.high.timer

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable.ArrayBuffer

/**
 * 不支持：
 * java.lang.UnsupportedOperationException: Setting timers is only supported on a keyed streams.
 */
object TimerProcessTest {

  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(4)

    // 需要先启动`nc -lk 9999`，用来发送数据，windows使用`nc -l -p 9999`命令
    val text: DataStream[String] = env.socketTextStream("localhost", 9999)

    val fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    val rst = text
      .process(new ProcessFunction[String, String] {
        val values = new ArrayBuffer[String]()

        override def processElement(value: String, ctx: ProcessFunction[String, String]#Context, out: Collector[String]): Unit = {
          val currentProcessingTime: Long = ctx.timerService().currentProcessingTime()
          println("currentProcessingTime", fmt.format(new Date(currentProcessingTime)))

          ctx.timerService().registerProcessingTimeTimer(currentProcessingTime + 3000)

          values += value
        }

        override def onTimer(timestamp: Long, ctx: ProcessFunction[String, String]#OnTimerContext, out: Collector[String]): Unit = {
          println("onTimer", fmt.format(new Date(timestamp)))
          println(values)
        }
      })


    rst.print()

    env.execute("WordCountTumblingWindow")
  }

}
