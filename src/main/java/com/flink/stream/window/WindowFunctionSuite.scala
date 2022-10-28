package com.flink.stream.window

import java.text.SimpleDateFormat
import java.util.Date

import com.flink.base.FlinkBaseSuite
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.util.Collector

/**
 * https://nightlies.apache.org/flink/flink-docs-release-1.16/zh/docs/dev/datastream/operators/windows/#%E7%AA%97%E5%8F%A3%E5%87%BD%E6%95%B0window-functions
 * https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/dev/datastream/operators/windows/#window-functions
 *
 * 定义了 window assigner 之后，我们需要指定当窗口触发之后，我们如何计算每个窗口中的数据， 这就是 window function 的职责了。关于窗口如何触发，详见 triggers。
 *
 * 窗口函数有三种：ReduceFunction、AggregateFunction 或 ProcessWindowFunction。
 * 前两者执行起来更高效（详见 State Size）因为 Flink 可以在每条数据到达窗口后 进行增量聚合（incrementally aggregate）。
 * 而 ProcessWindowFunction 会得到能够遍历当前窗口内所有数据的 Iterable，以及关于这个窗口的 meta-information。
 *
 * 使用 ProcessWindowFunction 的窗口转换操作没有其他两种函数高效，因为 Flink 在窗口触发前必须缓存里面的所有数据。
 * ProcessWindowFunction 可以与 ReduceFunction 或 AggregateFunction 合并来提高效率。
 * 这样做既可以增量聚合窗口内的数据，又可以从 ProcessWindowFunction 接收窗口的 metadata。
 */
class WindowFunctionSuite extends FlinkBaseSuite{
  // 每秒2个，5秒10个
  override def parallelism: Int = 2

  /**
   * ProcessWindowFunction 有能获取包含窗口内所有元素的 Iterable， 以及用来获取时间和状态信息的 Context 对象，比其他窗口函数更加灵活。
   * ProcessWindowFunction 的灵活性是以性能和资源消耗为代价的， 因为窗口中的数据无法被增量聚合，而需要在窗口触发前缓存所有数据。
   */
  test("ProcessWindowFunction"){
    val onlineLog = env.addSource(new OnlineLogSouce(1, 1000, 1))
    println("Source parallelism:" + onlineLog.parallelism)

    type R = (String, Int, String, String)
    val processWindowFunction = new ProcessWindowFunction[OnlineLog, R, String, TimeWindow] {
      val fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      def process(key: String, context: Context, elements: Iterable[OnlineLog], out: Collector[R]): Unit = {
        val visitCnt = elements.map(_.visitCnt).sum
        val windowStart = fmt.format(new Date(context.window.getStart))
        val windowEnd = fmt.format(new Date(context.window.getEnd))
        out.collect((key, visitCnt, windowStart, windowEnd))
      }
    }

    onlineLog
      .keyBy(_.pageId)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .process[R](processWindowFunction)
      .print()
  }

  /**
   * ReduceFunction 指定两条输入数据如何合并起来产生一条输出数据，输入和输出数据的类型必须相同。 Flink 使用 ReduceFunction 对窗口中的数据进行增量聚合。
   * 输入和输出数据的类型必须相同：决定了这个方法用的较少
   */
  test("ReduceFunction"){
    val onlineLog = env.addSource(new OnlineLogSouce(1, 1000, 1))
    println("Source parallelism:" + onlineLog.parallelism)

    onlineLog
      .keyBy(_.pageId)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .reduce{(log1, log2) =>
        // log1就是返回的被聚合的元素，和scala集合的reduce一样
        println("log1:" + log1.timeStr + "," + log1.visitCnt)
        log2.copy(visitCnt = log2.visitCnt + log1.visitCnt)
      }
      .print()

  }

  /**
   * ReduceFunction 是 AggregateFunction 的特殊情况。
   * AggregateFunction 接收三个类型：输入数据的类型(IN)、累加器的类型（ACC）和输出数据的类型（OUT）。
   * 输入数据的类型是输入流的元素类型，AggregateFunction 接口有如下几个方法：创建初始累加器、把每一条元素加进累加器、合并两个累加器、从累加器中提取输出（OUT 类型）。
   *
   * 与 ReduceFunction 相同，Flink 会在输入数据到达窗口时直接进行增量聚合。
   */
  test("AggregateFunction"){
    val onlineLog = env.addSource(new OnlineLogSouce(1, 1000, 1))
    println("Source parallelism:" + onlineLog.parallelism)

    type ACC = (String, Int)
    type R = (String, Int)
    val aggregateFunction = new AggregateFunction[OnlineLog, ACC, R] {
      def createAccumulator(): (String, Int) = ("", 0)

      def add(value: OnlineLog, accumulator: (String, Int)): (String, Int) = (value.pageId, value.visitCnt + accumulator._2)

      def merge(a: (String, Int), b: (String, Int)): (String, Int) = (a._1, a._2 + b._2)

      def getResult(accumulator: (String, Int)): (String, Int) = accumulator

    }

    onlineLog
      .keyBy(_.pageId)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .aggregate(aggregateFunction)
      .print()
  }

  test("AggregateFunction结合ProcessWindowFunction"){
    val onlineLog = env.addSource(new OnlineLogSouce(1, 1000, 1))
    println("Source parallelism:" + onlineLog.parallelism)

    type ACC = Int
    type R = Int
    val aggregateFunction = new AggregateFunction[OnlineLog, ACC, R] {
      def createAccumulator(): ACC = 0

      def add(value: OnlineLog, accumulator: ACC): ACC = value.visitCnt + accumulator

      def getResult(accumulator: ACC): R = accumulator

      def merge(a: ACC, b: ACC): ACC = a + b
    }
    type R2 = (String, Int, String, String)
    val processWindowFunction = new ProcessWindowFunction[R, R2, String, TimeWindow] {
      val fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      def process(key: String, context: Context, elements: Iterable[R], out: Collector[R2]): Unit = {
        val visitCnt = elements.head
        val windowStart = fmt.format(new Date(context.window.getStart))
        val windowEnd = fmt.format(new Date(context.window.getEnd))
        out.collect((key, visitCnt, windowStart, windowEnd))
      }
    }


    onlineLog
      .keyBy(_.pageId)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .aggregate(aggregateFunction, processWindowFunction)
      .print()
  }
}
