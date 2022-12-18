package com.flink.debug.stream.base

import com.flink.connector.localfile.LocalFileSourceFunction
import com.flink.serialization.{BinarySchema, JsonDataDeserializationSchema}
import com.flink.stream.func.DeserializeFunc
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.scalatest.funsuite.AnyFunSuite
import FuncOpenCloseSuite._
import com.flink.log.Logging
import org.apache.flink.api.common.functions.{RichFilterFunction, RichMapFunction}
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

class FuncOpenCloseSuite extends AnyFunSuite {

  /**
   * [[org.apache.flink.streaming.runtime.tasks.StreamTask]]
   *    初始化时：
   *    operatorChain.initializeStateAndOpenOperators(createStreamTaskStateInitializer());
   *  RegularOperatorChain.initializeStateAndOpenOperators:
   *   反向遍历operatorChain的每个operator，调用初始化方法
   *   (operatorWrapper <- getAllOperators(true)){
   *      operator = operatorWrapper.getStreamOperator();
   *      operator.initializeState(streamTaskStateInitializer);
   *      operator.open();
   *   }
   *
   *   初始化时：
   *   operatorChain.closeAllOperators();
   * RegularOperatorChain.closeAllOperators:
   *   反向遍历operatorChain的每个operator，调用close方法
   *   (operatorWrapper <- getAllOperators(true)){
   *      operatorWrapper.close() // wrapped.close()
   *   }
   */
  test("funcOpenClose"){
    val conf = new Configuration()
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(2)
    env.getConfig.enableObjectReuse()
    //val tEnv = StreamTableEnvironment.create(env)

    val ds = env.addSource(new LocalFileSourceFunction[Array[Byte]]("D:\\IdeaWorkspace\\FlinkNote1.16\\files\\online_log.json",
      sleep = 2000, deserializer = new BinarySchema))

    val rstDs = ds.map(new DeserializeFunc(new JsonDataDeserializationSchema[Log](classOf[Log], true)))
      .filter(new FilterFunc)
      .map(new MapFunc1)
      .map(new MapFunc2)
      .addSink(new SinkFunc)
      .name("sink")

    env.execute("funcOpenClose")
  }

}

object FuncOpenCloseSuite {
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

  class FilterFunc extends RichFilterFunction[Log] with Logging{
    override protected def logName: String = "com.flink.FilterFunc"

    override def open(parameters: Configuration): Unit = {
      logInfo("open")
    }

    override def close(): Unit = {
      logInfo("close")
    }

    override def filter(log: Log): Boolean = {
      log.bs == "hotel" && log.item_id != null
    }
  }

  class MapFunc1 extends RichMapFunction[Log, (String, String, Long, String)]  with Logging{
    override protected def logName: String = "com.flink.MapFunc1"

    override def open(parameters: Configuration): Unit = {
      logInfo("open")
    }

    override def close(): Unit = {
      logInfo("close")
    }

    override def map(log: Log): (String, String, Long, String) = {
      (log.page_id, log.page_name, log.item_id, log.visit_time)
    }
  }

  class MapFunc2 extends RichMapFunction[(String, String, Long, String), (String, String, Long, String)]  with Logging{
    override protected def logName: String = "com.flink.MapFunc2"

    override def open(parameters: Configuration): Unit = {
      logInfo("open")
    }

    override def close(): Unit = {
      logInfo("close")
    }

    override def map(log: (String, String, Long, String)): (String, String, Long, String) = {
      log
    }
  }

  class SinkFunc extends RichSinkFunction[(String, String, Long, String)] with Logging{
    override protected def logName: String = "com.flink.SinkFunc"

    override def open(parameters: Configuration): Unit = {
      1
      logInfo("open")
    }

    override def close(): Unit = {
      1
      logInfo("close")
    }

    override def invoke(value: (String, String, Long, String), context: SinkFunction.Context): Unit = {
      println(value)
    }
  }
}
