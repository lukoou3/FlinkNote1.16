package com.flink.stream.base

import com.flink.base.FlinkBaseSuite
import com.flink.stream.source.UniqueIdSequenceSource
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/dev/datastream/side_output/
 * 除了从DataStream operations产生主流结果流之外，可以产生任意数量的SideOutput结果流。
 * SideOutput结果流不需必须与主流中的数据的类型相同，不同的SideOutput结果流输出的类型也可以不同。
 * 当想要通过复制一个流并从中过滤流拆分数据流，SideOutput更加高效。
 *
 * 支持SideOutput输出流的函数(Process)：
 *    ProcessFunction
 *    KeyedProcessFunction
 *    CoProcessFunction
 *    KeyedCoProcessFunction
 *    ProcessWindowFunction
 *    ProcessAllWindowFunction
 *
 *
 */
class SideOutputSuite extends FlinkBaseSuite{
  override def parallelism: Int = 1

  test("SideOutput"){
    val ds = env.addSource(new UniqueIdSequenceSource(sleepMillis = 1000))
    val outputTag1 = OutputTag[Int]("side-output1")
    val outputTag2 = OutputTag[String]("side-output2")

    // 元素可以发送到不同的输出中
    val mainStream: DataStream[String] = ds.map(_.toInt).process(new ProcessFunction[Int, String] {
      override def processElement(value: Int, ctx: ProcessFunction[Int, String]#Context, out: Collector[String]): Unit = {
        // 发送数据到主要的输出
        out.collect("main-" + value.toString)

        // 发送数据到旁路输出
        if (value % 3 == 0) {
          ctx.output(outputTag1, value)
        }
        if (value % 4 == 0) {
          ctx.output(outputTag2, "sideout2-" + value)
        }
      }
    })

    // 可以在 DataStream 运算结果上使用 getSideOutput(OutputTag) 方法获取旁路输出流。这将产生一个与旁路输出流结果类型一致的 DataStream：
    val sideOutputStream1: DataStream[Int] = mainStream.getSideOutput(outputTag1)
    val sideOutputStream2: DataStream[String] = mainStream.getSideOutput(outputTag2)

    mainStream.print()
    sideOutputStream1.print()
    sideOutputStream2.print()

    env.execute("SideOutput")
  }

}
