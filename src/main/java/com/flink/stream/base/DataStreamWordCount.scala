package com.flink.stream.base

// 和spark一样，需要引入隐式转换

import org.apache.flink.configuration.DeploymentOptions
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

import com.flink.utils.Converters._

object DataStreamWordCount {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // local
    println(env.getConfiguration.getOptional(DeploymentOptions.TARGET).asScala.getOrElse(""))

    // 需要先启动`nc -lk 9999`，用来发送数据，windows使用`nc -l -p 9999`命令
    val text: DataStream[String] = env.socketTextStream("localhost", 9999)
    println("Source parallelism:" + text.parallelism)

    val counts: DataStream[(String, Int)] = text
      .flatMap(_.toLowerCase.split("\\s+"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .keyBy(_._1)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .sum(1)

    counts.print()

    env.execute("Window Stream WordCount")
  }

}
