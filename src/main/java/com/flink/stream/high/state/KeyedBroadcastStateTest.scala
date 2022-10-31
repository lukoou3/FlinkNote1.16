package com.flink.stream.high.state

import org.apache.flink.api.common.state.{BroadcastState, MapStateDescriptor, ReadOnlyBroadcastState}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object KeyedBroadcastStateTest {

  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(4)

    val text1: DataStream[String] = env.socketTextStream("localhost", 9999)
    val text2: DataStream[String] = env.socketTextStream("localhost", 9988)

    // 1,1,莫南,17
    // 1,2,燕青丝,16
    // 0,1,莫南,17
    // 模拟维度表添加，删除
    val dimStream: DataStream[(Int, Int, (String, Int))] = text1.flatMap{
      line =>
      try {
        val arrays = line.trim.split(",")
        if (arrays.length >= 4) {
          val operate = arrays(0).toInt
          val id = arrays(1).toInt
          val name = arrays(2)
          val age = arrays(3).toInt
          println(s"source:$line")
          Some((operate, id, (name, age)))
        } else None
      } catch {
        case e: Exception =>
          e.printStackTrace()
          None
      }
    }

    // 1,page1
    // 2,page1
    // 1,page2
    // 2,page2
    // 事实表
    val stream: KeyedStream[(Int, String), String] = text2.flatMap {
      line =>
        try {
          val arrays = line.trim.split(",")
          if (arrays.length >= 2) {
            val id = arrays(0).toInt
            val page = arrays(1)
            println(s"source:$line")
            Some((id, page))
          } else None
        } catch {
          case e: Exception =>
            e.printStackTrace()
            None
        }
    }.keyBy(_._2);

    val mapStateDescriptor = new MapStateDescriptor[Int, (String, Int)]("dim-state", createTypeInformation[Int], createTypeInformation[(String, Int)])
    val broadcastStream: BroadcastStream[(Int, Int, (String, Int))] = dimStream.broadcast(mapStateDescriptor)

    val rst: DataStream[String] = stream.connect(broadcastStream)
      .process(new BroadcastProcess(mapStateDescriptor))

    rst.print()

    env.execute("BroadcastStateTest")
  }

  class BroadcastProcess(mapStateDescriptor: MapStateDescriptor[Int, (String, Int)]) extends KeyedBroadcastProcessFunction[String, (Int, String), (Int, Int, (String, Int)), String] {
    type KS = String
    type IN1 = (Int, String)
    type IN2 = (Int, Int, (String, Int))
    type OUT = String

    override def processElement(value: IN1, ctx: KeyedBroadcastProcessFunction[KS, IN1, IN2, OUT]#ReadOnlyContext, out: Collector[OUT]): Unit = {
      val brocast: ReadOnlyBroadcastState[Int, (String, Int)] = ctx.getBroadcastState(mapStateDescriptor)
      val (id, page) = value
      brocast.get(id) match {
        case (name, age) =>
          out.collect(page + "," + id  + "," + name + "," + age)
        case _ =>
          out.collect("未关联到:" + id)
      }
    }

    override def processBroadcastElement(value: IN2, ctx: KeyedBroadcastProcessFunction[KS, IN1, IN2, OUT]#Context, out: Collector[OUT]): Unit = {
      val brocast: BroadcastState[Int, (String, Int)] = ctx.getBroadcastState(mapStateDescriptor)
      val (operate, id, (name, age)) = value
      operate match {
        case 1 =>
          brocast.put(id, (name, age))
        case 0 =>
          brocast.remove(id)
        case _ =>
      }
    }
  }

}
