package com.flink.stream.high.timer

import com.flink.base.FlinkBaseSuite
import com.flink.stream.source.UniqueIdSequenceSource
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/dev/datastream/operators/process_function/#timers
 *
 * 两种类型的计时器（处理时间和事件时间）都由内部TimerService维护并排队执行。
 * 错误的!!!：每个key只维护一个定时时间戳，为单个key注册多个定时器，会覆盖之前的定时器。
 * 正确的：每个键和时间戳的TimerService重复数据删除定时器，即每个键和时间戳最多有一个定时器。如果为同一个时间戳注册了多个计时器，则该onTimer()方法将只调用一次。
 * 为某个key连续设置多个定时器，这些定时器都会触发。
 * Flink 同步onTimer()和processElement()的调用。因此，用户不必担心并发修改状态。
 *
 * 容错(Fault Tolerance)：
 *    计时器具有容错性，并与应用程序的状态一起设置检查点。如果发生故障恢复或从保存点启动应用程序时，将恢复计时器。
 *    应该在恢复之前触发的检查点处理时间计时器将立即触发。当应用程序从故障中恢复或从保存点启动时，可能会发生这种情况。
 *
 * 定时器合并(Timer Coalescing):
 *   由于 Flink 只为每个键和时间戳维护一个计时器，因此您可以通过降低计时器分辨率来合并它们来减少计时器的数量。
 *   对于 1 秒的计时器分辨率（事件或处理时间），您可以将目标时间向下舍入到整秒。计时器将最多提前 1 秒触发，但不迟于毫秒精度的请求。因此，每个键和秒最多有一个计时器。
 *      val coalescedTime = ((ctx.timestamp + timeout) / 1000) * 1000
 *      ctx.timerService.registerProcessingTimeTimer(coalescedTime)
 *   由于事件时间计时器仅在水印进入时触发，您还可以使用当前的水印来安排这些计时器并将其与下一个水印合并：
 *      val coalescedTime = ctx.timerService.currentWatermark + 1
 *      ctx.timerService.registerEventTimeTimer(coalescedTime)
 *
 * 定时器也可以按如下方式停止和删除：
 *   停止处理时间计时器：ctx.timerService.deleteProcessingTimeTimer(timestampOfTimerToStop)
 *   停止事件时间计时器：ctx.timerService.deleteEventTimeTimer(timestampOfTimerToStop)
 */
class TimerSuite extends FlinkBaseSuite {
  def parallelism: Int = 1

  /**
   * 这个例子，元素一秒产生一个，定时设置的正秒的4秒后，本来以为每个key的定时器是覆盖的，发现每个注册的定时器都会触发
   * 看下源码：
   *  [[org.apache.flink.streaming.api.SimpleTimerService]]
   *  [[org.apache.flink.streaming.api.operators.InternalTimerServiceImpl]]
   *  InternalTimerServiceImpl定时器使用队列存的：processingTimeTimersQueue属性
   *
   */
  test("TimerCoalesc"){
    val ds = env.addSource(new UniqueIdSequenceSource(sleepMillis = 1000))

    ds.keyBy(_ => 1)
      .process(new KeyedProcessFunction[Int, Long, String] {
        def processElement(value: Long, ctx: KeyedProcessFunction[Int, Long, String]#Context, out: Collector[String]): Unit = {
          val service = ctx.timerService()
          val ts = service.currentProcessingTime()
          val time =  ts / 1000  * 1000 + 4000
          service.registerProcessingTimeTimer(time)
          println(ts, time, value)
        }

        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Int, Long, String]#OnTimerContext, out: Collector[String]): Unit = {
          println("onTimer", timestamp)
        }
      })
      .print()

  }

}
