package com.flink.debug.stream.base

import com.flink.base.FlinkBaseSuite
import com.flink.connector.localfile.LocalFileSourceFunction
import com.flink.debug.stream.base.DsBaseSuite._
import com.flink.serialization.{BinarySchema, JsonDataDeserializationSchema}
import com.flink.stream.func.DeserializeFunc
import org.apache.flink.api.common.functions.{FilterFunction, MapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.formats.json.JsonDeserializationSchema
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.scalatest.funsuite.AnyFunSuite

class DsBaseSuite extends AnyFunSuite {

  /**
   * 构造函数：[[org.apache.flink.runtime.deployment.TaskDeploymentDescriptor]]
   * [[org.apache.flink.streaming.runtime.tasks.StreamTask#invoke()]]
   */
  test("simple_etl_enableObjectReuse"){
    val conf = new Configuration()
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(2)
    env.getConfig.enableObjectReuse()
    //val tEnv = StreamTableEnvironment.create(env)

    val ds = env.addSource(new LocalFileSourceFunction[Array[Byte]]("D:\\IdeaWorkspace\\FlinkNote1.16\\files\\online_log.json",
      sleep = 1000, deserializer = new BinarySchema))

    val rstDs = ds.map(new DeserializeFunc(new JsonDataDeserializationSchema[Log](classOf[Log], true)))
      .filter(new FilterFunc)
      .map(new MapFunc1)
      .map(new MapFunc2)
      .addSink(new SinkFunc)
      .name("sink")

    env.execute("simple etl")
  }

  /**
   * 构造函数：[[org.apache.flink.runtime.deployment.TaskDeploymentDescriptor]]
   * [[org.apache.flink.streaming.runtime.tasks.StreamTask#invoke()]]
   *
   * [[org.apache.flink.api.scala.typeutils.CaseClassSerializer.copy]]chain内部
   * [[org.apache.flink.api.scala.typeutils.CaseClassSerializer.serialize]]chain之间
   */
  test("simple_etl_disableObjectReuse"){
    val conf = new Configuration()
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(2)
    //env.getConfig.enableObjectReuse()
    //val tEnv = StreamTableEnvironment.create(env)

    val ds = env.addSource(new LocalFileSourceFunction[Array[Byte]]("D:\\IdeaWorkspace\\FlinkNote1.16\\files\\online_log.json",
      sleep = 1000, deserializer = new BinarySchema))

    val rstDs = ds.map(new DeserializeFunc(new JsonDataDeserializationSchema[Log](classOf[Log], true)))
      .filter(new FilterFunc)
      .map(new MapFunc1)
      .map(new MapFunc2)
      .addSink(new SinkFunc)
      .name("sink")

    env.execute("simple etl")
  }

  /**
   * 构造函数：[[org.apache.flink.runtime.deployment.TaskDeploymentDescriptor]]
   * [[org.apache.flink.streaming.runtime.tasks.StreamTask#invoke()]]
   *
   * [[org.apache.flink.api.scala.typeutils.CaseClassSerializer.copy]]chain内部
   * [[org.apache.flink.api.scala.typeutils.CaseClassSerializer.serialize]]chain之间
   */
  test("simple_etl_disableChain"){
    val conf = new Configuration()
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(2)
    //env.getConfig.enableObjectReuse()
    //val tEnv = StreamTableEnvironment.create(env)

    val ds = env.addSource(new LocalFileSourceFunction[Array[Byte]]("D:\\IdeaWorkspace\\FlinkNote1.16\\files\\online_log.json",
      sleep = 1000, deserializer = new BinarySchema))

    val rstDs = ds.map(new DeserializeFunc(new JsonDataDeserializationSchema[Log](classOf[Log], true)))
      .filter(new FilterFunc)
      .map(new MapFunc1).startNewChain()
      .map(new MapFunc2)
      .addSink(new SinkFunc)
      .name("sink")

    env.execute("simple etl")
  }

}

object DsBaseSuite {
  case class Log(
    dt: String,
    bs: String,
    report_time: String,
    browser_uniq_id: String,
    os_plant: String,
    page_id: String,
    page_name: String,
    page_param: String,
    item_id: java.lang.Long,
    item_type: java.lang.Integer,
    visit_time: String
  )

  class FilterFunc extends FilterFunction[Log]{
    override def filter(log: Log): Boolean = {
      log.bs == "hotel" && log.item_id != null
    }
  }

  class MapFunc1 extends MapFunction[Log, (String, String, Long, String)] {
    override def map(log: Log): (String, String, Long, String) = {
      (log.page_id, log.page_name, log.item_id, log.visit_time)
    }
  }

  class MapFunc2 extends MapFunction[(String, String, Long, String), (String, String, Long, String)] {
    override def map(log: (String, String, Long, String)): (String, String, Long, String) = {
      log
    }
  }

  class SinkFunc extends SinkFunction[(String, String, Long, String)]{
    override def invoke(value: (String, String, Long, String), context: SinkFunction.Context): Unit = {
      println(value)
    }
  }

}