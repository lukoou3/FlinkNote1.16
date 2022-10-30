package com.flink.stream.join

import java.lang

import com.flink.base.FlinkBaseSuite
import org.apache.flink.api.common.state.{StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import CoProcessJoinSuite._

/**
 * 这里使用connect加上KeyedCoProcessFunction实现双流join，两个流互相等待60秒
 * 对于3秒未关联到的打印出来，现实业务中可以把关联不到读取hbase或者数据库关联，也可以输出到测流在测流中关联
 * 这里设置OnReadAndWrite(读取也不失效)，具体根据业务情况设置
 */
class CoProcessJoinSuite extends FlinkBaseSuite {
  override def parallelism: Int = 2

  test("CoProcessJoin"){
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
    }

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
    }

    val rst = stream1.keyBy(_.id).connect(stream2.keyBy(_.id))
      .process(new KeyedCoProcessFunction[Int, Data1, Data2, Data] {
        var data1State: ValueState[Data1] = _
        var data2State: ValueState[Data2] = _
        var timeState: ValueState[lang.Long] = _

        override def open(parameters: Configuration): Unit = {
          // 这里设置OnReadAndWrite，具体根据业务情况设置
          val ttlConfig = StateTtlConfig
            .newBuilder(Time.seconds(60))
            .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build

          val data1StateDescriptor = new ValueStateDescriptor[Data1]("data1-state", classOf[Data1])
          val data2StateDescriptor = new ValueStateDescriptor[Data2]("data2-state", classOf[Data2])
          data1StateDescriptor.enableTimeToLive(ttlConfig)
          data2StateDescriptor.enableTimeToLive(ttlConfig)
          data1State = getRuntimeContext.getState(data1StateDescriptor)
          data2State = getRuntimeContext.getState(data2StateDescriptor)
          timeState = getRuntimeContext.getState(new ValueStateDescriptor[lang.Long]("time-state", classOf[lang.Long]))
        }

        override def processElement1(data1: Data1, ctx: KeyedCoProcessFunction[Int, Data1, Data2, Data]#Context, out: Collector[Data]): Unit = {
          data1State.update(data1)

          val data2 = data2State.value()
          if(data2 != null){
            out.collect(Data(data1.id, data1.name, data2.age))

            if(timeState.value() != null){
              ctx.timerService().deleteProcessingTimeTimer(timeState.value())
              timeState.clear()
            }
          }else if(timeState.value() == null){
            val time = ctx.timerService().currentProcessingTime() + 3000
            ctx.timerService().registerProcessingTimeTimer(time)
            timeState.update(time)
          }
        }

        override def processElement2(data2: Data2, ctx: KeyedCoProcessFunction[Int, Data1, Data2, Data]#Context, out: Collector[Data]): Unit = {
          data2State.update(data2)

          val data1 = data1State.value()
          if(data1 != null){
            out.collect(Data(data1.id, data1.name, data2.age))

            if(timeState.value() != null){
              ctx.timerService().deleteProcessingTimeTimer(timeState.value())
              timeState.clear()
            }
          }else if(timeState.value() == null){
            val time = ctx.timerService().currentProcessingTime() + 3000
            ctx.timerService().registerProcessingTimeTimer(time)
            timeState.update(time)
          }
        }

        override def onTimer(timestamp: Long, ctx: KeyedCoProcessFunction[Int, Data1, Data2, Data]#OnTimerContext, out: Collector[Data]): Unit = {
          timeState.clear()
          if(data1State.value() != null && data2State.value() != null){

          }else{
            if(data1State.value() != null){
              println("data1未关联到", data1State.value())
            }
            if(data2State.value() != null){
              println("data2未关联到", data2State.value())
            }
          }
        }
      })

    rst.print()

    env.execute("CoProcessJoin")
  }

}

object CoProcessJoinSuite{
  case class Data(id: Int, name: String, age: Int)
  case class Data1(id: Int, name: String)
  case class Data2(id: Int, age: Int)
}
