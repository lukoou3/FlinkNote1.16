package com.flink.stream.base

import com.flink.base.FlinkBaseSuite
import com.flink.stream.source.UniqueIdSequenceSource
import org.apache.flink.streaming.api.scala._

class DataStreamsTransformationsSuite extends FlinkBaseSuite{
  override def parallelism: Int = 2

  test("union"){
    val ds1 = env.addSource(new UniqueIdSequenceSource(sleepMillis = 1000)).setParallelism(1).map("1:" + _).setParallelism(1)
    val ds2 = env.addSource(new UniqueIdSequenceSource(sleepMillis = 1000)).setParallelism(1).map("2:" + _).setParallelism(1)
    val ds3 = env.addSource(new UniqueIdSequenceSource(sleepMillis = 1000)).setParallelism(1).map("3:" + _).setParallelism(1)

    val ds = ds1.union(ds2, ds3)
    ds.print()
  }

  test("connect"){
    val ds1 = env.addSource(new UniqueIdSequenceSource(sleepMillis = 1000)).setParallelism(1)
      .map(x => (x, 1)).setParallelism(1)
    val ds2 = env.addSource(new UniqueIdSequenceSource(sleepMillis = 1000)).setParallelism(1)
      .map(x => (x, "2")).setParallelism(1)

    // ConnectedStreams不是DataStream子类没有print方法
    val ds: ConnectedStreams[(Long, Int), (Long, String)] = ds1.connect(ds2)
    // 类似union后map
    val rst = ds.map[(Long, String)]((x: (Long, Int)) => (x._1, x._2.toString), (x: (Long, String)) => x)
    rst.print()
  }



}
