package com.flink.stream.base

import com.flink.base.FlinkBaseSuite
import com.flink.stream.source.UniqueIdSequenceSource
import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichMapFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable.ArrayBuffer

/**
 * https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/dev/datastream/operators/overview/
 *
 */
class BaseTransformationsSuite extends FlinkBaseSuite {

  override def parallelism: Int = 2

  /**
   * [[org.apache.flink.streaming.api.operators.StreamMap]].processElement:
   *    output.collect(element.replace(userFunction.map(element.getValue())))
   */
  test("map"){
    val ds = env.addSource(new UniqueIdSequenceSource(sleepMillis = 1000))
    ds.map(_.toInt)
      .addSink{data =>
        println(Thread.currentThread(), data)
      }
    env.execute("DataStreamTransformations")
  }

  /**
   * [[org.apache.flink.streaming.api.operators.StreamFlatMap]].processElement:
   *    collector.setTimestamp(element)
   *    userFunction.flatMap(element.getValue(), collector)
   */
  test("flatMap"){
    val ds = env.addSource(new UniqueIdSequenceSource(sleepMillis = 1000))
    ds.flatMap(new RichFlatMapFunction[Long, String] {
      lazy val index = getRuntimeContext.getIndexOfThisSubtask
      override def flatMap(value: Long, out: Collector[String]): Unit = {
        out.collect(s"$index:$value")
      }
    }).flatMap(x => Option(x))
      .addSink{data =>
        println(data)
      }
    env.execute("DataStreamTransformations")
  }

  /**
   * [[org.apache.flink.streaming.api.operators.StreamFilter]].processElement:
   *    if (userFunction.filter(element.getValue())) {
   *        output.collect(element);
   *    }
   */
  test("filter"){
    val ds = env.addSource(new UniqueIdSequenceSource(sleepMillis = 1000))
    ds.flatMap(new RichFlatMapFunction[Long, (Int, Long)] {
      lazy val index = getRuntimeContext.getIndexOfThisSubtask
      override def flatMap(value: Long, out: Collector[(Int, Long)]): Unit = {
        out.collect((index, value))
      }
    }).filter(_._1 == 0)
      .addSink{data =>
        println(data)
      }
    env.execute("DataStreamTransformations")
  }

  test("keyBy"){
    val ds = env.addSource(new UniqueIdSequenceSource(sleepMillis = 1000)).setParallelism(1)
    ds.map(_.toInt)
      .keyBy(x => x)
      .map(new RichMapFunction[Int, (Int, Seq[Int])] {
        lazy val index = getRuntimeContext.getIndexOfThisSubtask
        val datas = new ArrayBuffer[Int]()
        override def map(value: Int): (Int, Seq[Int]) = {
          datas += value
          (index, datas)
        }
      })
      .addSink{data =>
        println(data)
      }
    env.execute("DataStreamTransformations")
  }

  test("rebalance"){
    val ds = env.addSource(new UniqueIdSequenceSource(sleepMillis = 1000)).setParallelism(1)
    ds.map(_.toInt)
      .rebalance
      .map(new RichMapFunction[Int, (Int, Seq[Int])] {
        lazy val index = getRuntimeContext.getIndexOfThisSubtask
        val datas = new ArrayBuffer[Int]()
        override def map(value: Int): (Int, Seq[Int]) = {
          datas += value
          (index, datas)
        }
      })
      .addSink{data =>
        println(data)
      }
    env.execute("DataStreamTransformations")
  }
}
