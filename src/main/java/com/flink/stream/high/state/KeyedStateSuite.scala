package com.flink.stream.high.state

import java.{lang, util}
import java.util.Map

import com.flink.base.FlinkBaseSuite
import com.flink.log.Logging
import com.flink.stream.source.UniqueIdSequenceSource
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.runtime.BoxesRunTime
import scala.collection.JavaConverters._

/**
 * https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/dev/datastream/fault-tolerance/state/#using-keyed-state
 *
 * 状态通过 RuntimeContext 进行访问，因此只能在 rich functions 中使用。
 * state并不是只能在process function中使用，timers是只能在keyed stream的process function中使用。
 * 我自己使用state时基本都使用KeyedProcessFunction，使用state基本都使用timer，而且需要访问key。不使用KeyedProcessFunction最好使用ttl。
 *
 */
class KeyedStateSuite extends FlinkBaseSuite{
  override def parallelism: Int = 2

  test("ValueState_flatMap"){
    val ds = env.addSource(new UniqueIdSequenceSource(sleepMillis = 1000))

    ds.setParallelism(1)
      .map(x => ( (x % 4).toInt, x.toInt)).setParallelism(1)
      .keyBy(_._1)
      .flatMap(new RichFlatMapFunction[(Int, Int), String] {
        @transient lazy val cntState: ValueState[Int] = {
          val descriptor = new ValueStateDescriptor[Int]("cnt", createTypeInformation[Int])
          getRuntimeContext.getState(descriptor)
        }
        @transient lazy val sumState: ValueState[lang.Integer] = {
          val descriptor = new ValueStateDescriptor[lang.Integer]("sum", createTypeInformation[lang.Integer])
          getRuntimeContext.getState(descriptor)
        }
        @transient lazy val datasState: ValueState[List[Int]] = getRuntimeContext.getState(
          new ValueStateDescriptor[List[Int]]("datas", createTypeInformation[List[Int]]))

        override def flatMap(value: (Int, Int), out: Collector[String]): Unit = {
          // 这里不会抛出异常
          // scala.runtime.BoxesRunTime.unboxToInt(null)
          val cnt = cntState.value()
          val sum = sumState.value()
          val datas = datasState.value()
          // 第一次默认是0, null, null
          //println(cnt, sum, datas)

          cntState.update(cnt + 1)
          sumState.update(BoxesRunTime.unboxToInt(sum) + value._2)
          datasState.update(if(datas == null) List(value._2) else value._2 :: datas)

          out.collect(s"${value._1};${cntState.value()}: ${sumState.value()}; ${datasState.value()}")
        }
      })
      .print()

    env.execute("ValueState")
  }

  /**
   * 可以设置默认值，但竟然标记为Deprecated了
   */
  test("ValueState_defaultValue"){
    val ds = env.addSource(new UniqueIdSequenceSource(sleepMillis = 1000))

    ds.setParallelism(1)
      .map(x => ( (x % 4).toInt, x.toInt)).setParallelism(1)
      .keyBy(_._1)
      .flatMap(new RichFlatMapFunction[(Int, Int), String] {
        @transient lazy val cntState: ValueState[Int] = {
          val descriptor = new ValueStateDescriptor[Int]("cnt", createTypeInformation[Int], 0)
          getRuntimeContext.getState(descriptor)
        }
        @transient lazy val sumState: ValueState[lang.Integer] = {
          val descriptor = new ValueStateDescriptor[lang.Integer]("sum", createTypeInformation[lang.Integer], 0)
          getRuntimeContext.getState(descriptor)
        }
        @transient lazy val datasState: ValueState[List[Int]] = getRuntimeContext.getState(
          new ValueStateDescriptor[List[Int]]("datas", createTypeInformation[List[Int]], Nil))

        override def flatMap(value: (Int, Int), out: Collector[String]): Unit = {
          val cnt = cntState.value()
          val sum = sumState.value()
          val datas = datasState.value()

          cntState.update(cnt + 1)
          sumState.update(sum + value._2)
          datasState.update(value._2 :: datas)

          out.collect(s"${value._1};${cntState.value()}: ${sumState.value()}; ${datasState.value()}")
        }
      })
      .print()

    env.execute("ValueState_defaultValue")
  }

  /**
   * datasState.clear(), datasState.update(empty list)后或者初始化时datasState.get()都不会为null
   * 不用判断空指针异常
   */
  test("ListState"){
    // 需要先启动`nc -lk 9999`，用来发送数据，windows使用`nc -l -p 9999`命令
    val ds = env.socketTextStream("localhost", 9999)

    val processFunction = new KeyedProcessFunction[Int, Int, String] with Logging{
      @transient lazy val datasState: ListState[Int] = {
        val descriptor = new ListStateDescriptor[Int]("datas", createTypeInformation[Int])
        getRuntimeContext.getListState(descriptor)
      }

      override def processElement(value: Int, ctx: KeyedProcessFunction[Int, Int, String]#Context, out: Collector[String]): Unit = {
        val key = ctx.getCurrentKey
        val iterable = datasState.get()
        logWarning(("process", key, iterable, iterable.getClass).toString)
        logWarning(("process", key, iterable.asScala.toList).toString)
        datasState.add(value)

        val interval = 1000 * 5
        val time = ctx.timerService().currentProcessingTime() / interval * interval + interval
        ctx.timerService().registerProcessingTimeTimer(time)
      }

      override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Int, Int, String]#OnTimerContext, out: Collector[String]): Unit = {
        val key = ctx.getCurrentKey
        val iterable = datasState.get()
        logWarning(("onTimer", key, iterable, iterable.getClass).toString)
        logWarning(("onTimer", key, iterable.asScala.toList).toString)

        if (iterable.asScala.toList.length > 0) {
          out.collect(iterable.asScala.toList.toString())
          if (iterable.asScala.toList.length > 1) {
            datasState.update(iterable.asScala.toList.tail.asJava)
          }else{
            datasState.clear()
          }

          val interval = 1000 * 5
          val time = ctx.timerService().currentProcessingTime() / interval * interval + interval
          ctx.timerService().registerProcessingTimeTimer(time)
        }
      }
    }

    ds.setParallelism(1)
      .map(_.toInt).setParallelism(1)
      .keyBy(_ % 4)
      .process(processFunction)
      .print()

  }

  test("MapState") {
    // 需要先启动`nc -lk 9999`，用来发送数据，windows使用`nc -l -p 9999`命令
    val text = env.socketTextStream("localhost", 9999)

    val processFunction = new KeyedProcessFunction[String, String, String] with Logging {
      @transient lazy val mapState: MapState[String,Int] = {
        val descriptor = new MapStateDescriptor[String,Int]("datas", createTypeInformation[String], createTypeInformation[Int])
        getRuntimeContext.getMapState(descriptor)
      }

      override def processElement(value: String, ctx: KeyedProcessFunction[String, String, String]#Context, out: Collector[String]): Unit = {
        val entryIterable: lang.Iterable[Map.Entry[String, Int]] = mapState.entries()
        println(entryIterable, entryIterable.getClass)
        println(entryIterable.iterator().asScala.map(x => (x.getKey, x.getValue)).toMap)
        val iterator: util.Iterator[Map.Entry[String, Int]] = mapState.iterator()
        println(iterator, iterator.getClass)
        println(iterator.asScala.map(x => (x.getKey, x.getValue)).toMap)
        println("-" * 50)

        mapState.put(value, 1)
      }
    }

    text.keyBy(x => "1")
      .process(processFunction)
      .print()

  }


}
