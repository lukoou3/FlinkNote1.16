package com.flink.stream.high.state

import org.apache.flink.api.common.state.{BroadcastState, MapStateDescriptor, ReadOnlyBroadcastState}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/dev/datastream/fault-tolerance/broadcast_state/
 * https://nightlies.apache.org/flink/flink-docs-release-1.16/zh/docs/dev/datastream/fault-tolerance/broadcast_state/
 * 为了关联一个非广播流（keyed 或者 non-keyed）与一个广播流（BroadcastStream），我们可以调用非广播流的方法 connect()，并将 BroadcastStream 当做参数传入。
 * 这个方法的返回参数是 BroadcastConnectedStream，具有类型方法 process()，传入一个特殊的 CoProcessFunction 来书写我们的模式识别逻辑。
 * 具体传入 process() 的是哪个类型取决于非广播流的类型：
 *    如果流是一个 keyed 流，那就是 KeyedBroadcastProcessFunction 类型；
 *    如果流是一个 non-keyed 流，那就是 BroadcastProcessFunction 类型。
 *
 *  广播状态是可以恢复的看看[[org.apache.flink.streaming.api.operators.co.CoBroadcastWithNonKeyedOperator]]等
 *  有时间打断点看看吧
 *
 *  这两个其实用哪个都行，可以先用BroadcastProcessFunction关联，然后再调用keyBy。
 *  KeyedBroadcastProcessFunction中可以访问Keyed State和注册使用定时器。
 */
object BroadcastStateTest {

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
    // 事实表
    val stream: DataStream[(Int, String)] = text2.flatMap{
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
    }

    val mapStateDescriptor = new MapStateDescriptor[Int, (String, Int)]("dim-state", createTypeInformation[Int], createTypeInformation[(String, Int)])
    val broadcastStream: BroadcastStream[(Int, Int, (String, Int))] = dimStream.broadcast(mapStateDescriptor)

    val rst: DataStream[String] = stream.connect(broadcastStream)
      .process(new BroadcastProcess(mapStateDescriptor))

    rst.print()

    env.execute("BroadcastStateTest")
  }

  class BroadcastProcess(mapStateDescriptor: MapStateDescriptor[Int, (String, Int)]) extends BroadcastProcessFunction[(Int, String), (Int, Int, (String, Int)), String] {
    type IN1 = (Int, String)
    type IN2 = (Int, Int, (String, Int))
    type OUT = String

    override def processElement(value: IN1, ctx: BroadcastProcessFunction[IN1, IN2, OUT]#ReadOnlyContext, out: Collector[OUT]): Unit = {
      val brocast: ReadOnlyBroadcastState[Int, (String, Int)] = ctx.getBroadcastState(mapStateDescriptor)
      val (id, page) = value
      brocast.get(id) match {
        case (name, age) =>
          out.collect(id + "," + page + "," + name + "," + age)
        case _ =>
          out.collect("未关联到:" + id)
      }
    }

    override def processBroadcastElement(value: IN2, ctx: BroadcastProcessFunction[IN1, IN2, OUT]#Context, out: Collector[OUT]): Unit = {
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
