package com.flink.stream.high.timer

import java.text.SimpleDateFormat
import java.time.Duration
import java.util.Date

import org.apache.flink.api.common.eventtime._
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * 经过测试KeyedProcessFunction，每个key注册的timer是独立的
 * 每个key的注册的时间只在自己的key中触发
 * registerProcessingTimeTimer在系统时间到达时触发
 * registerEventTimeTimer在Watermark时间到达时触发
 */
object TimerKeyedProcessTest {

  /**
   * 注意scala函数泛型的位置，和java的不同
   * scala11不支持java8新加的语法(接口中的静态方法)，scala11可以通过加一个java的静态类间接调用，
   *    这里直接复制了WatermarkStrategy.forBoundedOutOfOrderness方法
   * 使用函数的柯力化可以自动类型推断
   */
  def watermarkStrategy[T](maxOutOfOrderness: Duration)(getTimestamp: T => Long): WatermarkStrategy[T] = {
    val watermarkStrategy = new WatermarkStrategy[T] {
      override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context): WatermarkGenerator[T] = {
        new BoundedOutOfOrdernessWatermarks[T](maxOutOfOrderness)
      }
    }

    // 这个返回的是一个新的对象
    val watermarkStrategyWithTimestampAssigner = watermarkStrategy.withTimestampAssigner(new SerializableTimestampAssigner[T] {
      override def extractTimestamp(element: T, recordTimestamp: Long): Long = {
        getTimestamp(element)
      }
    })

    watermarkStrategyWithTimestampAssigner
  }

  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(4)

    // 需要先启动`nc -lk 9999`，用来发送数据，windows使用`nc -l -p 9999`命令
    val text: DataStream[String] = env.socketTextStream("localhost", 9999)

    /**
     * 在本地使用socket测试多并行度时，一定要设置WithWatermark的Stream为1，注意Watermark的触发条件
     * 生产条件就不必考虑，毕竟实时的数据还是来的比较快的，数据量不会小
     */
    val words: DataStream[(Long, String)] = text.flatMap {
      line =>
        try {
          val arrays = line.trim.split("\\s+")
          if (arrays.length >= 2) {
            val time = arrays(0).toLong
            val word = arrays(1)
            println(s"source:$line")
            Some(time, word)
          } else None
        } catch {
          case e: Exception =>
            e.printStackTrace()
            None
        }
    }.setParallelism(1)

    val wordsWithWatermark = words.assignTimestampsAndWatermarks(
      watermarkStrategy[(Long, String)](Duration.ofSeconds(3))(_._1)
    )

    val fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    val rst = wordsWithWatermark
      .map(x => (x._2, 1))
      .keyBy(_._1)
      .process(new KeyedProcessFunction[String, (String, Int), (String, Int)] {
        private var cntState: ValueState[Integer] = _

        override def open(parameters: Configuration): Unit = {
          val stateDescriptor = new ValueStateDescriptor[Integer]("cnt-state", classOf[Integer])
          cntState = getRuntimeContext.getState(stateDescriptor)
        }

        override def processElement(value: (String, Int),
                                    ctx: KeyedProcessFunction[String, (String, Int), (String, Int)]#Context,
                                    out: Collector[(String, Int)]): Unit = {
          val cnt = Option(cntState.value()).getOrElse(0:Integer) + value._2
          cntState.update(cnt)

          //ctx.getCurrentKey
          val currentProcessingTime: Long = ctx.timerService().currentProcessingTime()
          val currentWatermark: Long = ctx.timerService().currentWatermark()
          println("currentProcessingTime", fmt.format(new Date(currentProcessingTime)))
          println("currentWatermark", fmt.format(new Date(currentWatermark)), currentWatermark)

          ctx.timerService().registerProcessingTimeTimer(currentProcessingTime + 3000)
          ctx.timerService().registerEventTimeTimer(currentWatermark + 1000)

          //out.collect((value._1, cnt))
        }

        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, (String, Int), (String, Int)]#OnTimerContext, out: Collector[(String, Int)]): Unit = {
          println("currentWatermark_onTimer", fmt.format(new Date(ctx.timerService().currentWatermark())))
          println("onTimer", fmt.format(new Date(timestamp)))

          out.collect((ctx.getCurrentKey, cntState.value()))
        }
      })


    rst.print()

    env.execute("WordCountTumblingWindow")
  }

}
