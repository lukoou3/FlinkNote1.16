package com.flink.stream.high.partition

import com.flink.base.FlinkBaseSuite
import com.flink.stream.source.IdSequenceSource
import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

import scala.collection.mutable

class PartitionSuite extends FlinkBaseSuite {
  def parallelism: Int = 4

  /**
   * keyBy: 相同的key被发送到相同的分区
   *  好像不是按照元素hashCode % numPartition
   * 看了下代码确实不是，keyby分区的计算公式是：MathUtils.murmurHash(key.hashCode()) % maxParallelism * parallelism / maxParallelism
   *    分区的类：[[org.apache.flink.streaming.runtime.partitioner.KeyGroupStreamPartitioner]]
   *    maxParallelism默认为128
   * 分区的计算和MapReduce、spark不一样，不知道为啥这样，但总体当key多时分布也是均匀的
   */
  test("keyBy"){
    val ds = env.addSource(new IdSequenceSource(sleepMillis = 3000L))

    (0 to 10).map(x => (x, x.hashCode())).foreach(println(_))

    ds.map{x =>  println(x); x }
      .keyBy(_._2)
      .addSink(new RichSinkFunction[(Int, Long)] {
        @transient lazy val idx = getRuntimeContext.getIndexOfThisSubtask
        val datas = new mutable.LinkedHashSet[Long]()
        override def invoke(value: (Int, Long), context: SinkFunction.Context): Unit = {
          datas += value._2
          println("sink:", idx, value, datas)
        }
      })

  }

  /**
   * rebalance: 轮训，肯定是均匀的。
   *  [[org.apache.flink.streaming.runtime.partitioner.RebalancePartitioner]]:
   *    nextChannelToSendTo = (nextChannelToSendTo + 1) % numberOfChannels
   */
  test("rebalance"){
    val ds = env.addSource(new IdSequenceSource(sleepMillis = 3000L)).setParallelism(1)

    ds.map{x =>  println(x); x }.setParallelism(1)
      .rebalance
      .addSink(new RichSinkFunction[(Int, Long)] {
        @transient lazy val idx = getRuntimeContext.getIndexOfThisSubtask
        override def invoke(value: (Int, Long), context: SinkFunction.Context): Unit = {
          println("sink:", idx, value)
        }
      })
  }

  /**
   * shuffle: 随机，数量多肯定也是均匀的，想要均匀直接rebalance就行，没必要shuffle。
   *  [[org.apache.flink.streaming.runtime.partitioner.ShufflePartitioner]]:
   *    random.nextInt(numberOfChannels)
   */
  test("shuffle"){
    val ds = env.addSource(new IdSequenceSource(sleepMillis = 3000L)).setParallelism(1)

    ds.map{x =>  println(x); x }.setParallelism(1)
      .shuffle
      .addSink(new RichSinkFunction[(Int, Long)] {
        @transient lazy val idx = getRuntimeContext.getIndexOfThisSubtask
        override def invoke(value: (Int, Long), context: SinkFunction.Context): Unit = {
          println("sink:", idx, value)
        }
      })
  }

  /**
   * partitionCustom, 自定义分区， 按照hash后取模， 和spark一样， 还是这样看着舒服
   */
  test("partitionCustom"){
    val ds = env.addSource(new IdSequenceSource(sleepMillis = 3000L))

    ds.map{x =>  println(x); x }
      .partitionCustom(new Partitioner[Long] {
        def partition(key: Long, numPartitions: Int): Int = key.hashCode().abs % numPartitions
      }, x => x._2)
      .addSink(new RichSinkFunction[(Int, Long)] {
        @transient lazy val idx = getRuntimeContext.getIndexOfThisSubtask
        val datas = new mutable.LinkedHashSet[Long]()
        override def invoke(value: (Int, Long), context: SinkFunction.Context): Unit = {
          datas += value._2
          println("sink:", idx, value, datas)
        }
      })
  }

}
