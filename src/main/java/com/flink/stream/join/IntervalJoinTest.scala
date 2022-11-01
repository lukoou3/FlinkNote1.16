package com.flink.stream.join

import java.time.Duration

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector


/**
 * https://nightlies.apache.org/flink/flink-docs-release-1.12/zh/dev/stream/operators/joining.html
 * https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/dev/datastream/operators/joining/
 *
 *
 * 就是使用的状态加定时器, 自己完全可以实现
 * [[org.apache.flink.streaming.api.operators.co.IntervalJoinOperator]]
 * 内部使用的事件时间
 */
object IntervalJoinTest {

  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)
    //env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    //env.getConfig.setAutoWatermarkInterval(0)

    // 需要先启动`nc -lk 9999`，用来发送数据，windows使用`nc -l -p 9999`命令
    val text1: DataStream[String] = env.socketTextStream("localhost", 9999)
    val text2: DataStream[String] = env.socketTextStream("localhost", 9988)
    val stream1 = text1.flatMap {
      line =>
        try {
          val arrays = line.trim.split("\\s+")
          if (arrays.length >= 2) {
            val id = arrays(0).toInt
            val name = arrays(1)
            println(s"source:$line")
            Some(Data1(id, name))
          } else None
        } catch {
          case e: Exception =>
            e.printStackTrace()
            None
        }
    }.assignTimestampsAndWatermarks(
      WatermarkStrategy.forBoundedOutOfOrderness[Data1](Duration.ofSeconds(0))
        .withTimestampAssigner(new SerializableTimestampAssigner[Data1] {
          override def extractTimestamp(element: Data1, recordTimestamp: Long): Long = {
            System.currentTimeMillis()
          }
        })
    )

    val stream2 = text2.flatMap {
      line =>
        try {
          val arrays = line.trim.split("\\s+")
          if (arrays.length >= 2) {
            val id = arrays(0).toInt
            val age = arrays(1).toInt
            println(s"source:$line")
            Some(Data2(id, age))
          } else None
        } catch {
          case e: Exception =>
            e.printStackTrace()
            None
        }
    }.assignTimestampsAndWatermarks(
      WatermarkStrategy.forBoundedOutOfOrderness[Data2](Duration.ofSeconds(0))
        .withTimestampAssigner(new SerializableTimestampAssigner[Data2] {
          override def extractTimestamp(element: Data2, recordTimestamp: Long): Long = {
            System.currentTimeMillis()
          }
        })
    )

    val ds: DataStream[Data] = stream1.keyBy(_.id)
      .intervalJoin(stream2.keyBy(_.id))
      .between(Time.seconds(-60), Time.seconds(60))
      .process(new ProcessJoinFunction[Data1, Data2, Data] {
        override def processElement(data1: Data1, data2: Data2, ctx: ProcessJoinFunction[Data1, Data2, Data]#Context, out: Collector[Data]): Unit = {
          out.collect(Data(data1.id, data1.name, data2.age))
        }
      })

    ds.print()

    env.execute("IntervalJoinTest")
  }


  case class Data(id: Int, name: String, age: Int)

  case class Data1(id: Int, name: String)

  case class Data2(id: Int, age: Int)

}
