package com.flink.sql.dsintegration

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

/**
 * https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/dev/table/data_stream_api/
 * 很多时候单独使用ds和table api并不是最好的选择，把ds和table api结合起来混合编程有时候很有用，可以充分发挥ds和table api各自的有点
 *
 * Dependencies and Imports：需要引入的依赖：flink-table-api-scala-bridge_2.12
 *
 * Flink提供了一个专门的StreamTableEnvironment，用于与DataStream API集成。
 * StreamTableEnvironment扩展了常规的TableEnvironment，并将DataStream API中使用的StreamExecutionEnvironments作为参数。
 *
 * StreamTableEnvironment会继承使用StreamExecutionEnvironment的配置，但是在StreamTableEnvironment实例化修改StreamExecutionEnvironment的配置，StreamTableEnvironment并不一定会生效
 * 所以建议在StreamTableEnvironment实例化之前设置完StreamExecutionEnvironment的配置
 *
 * Execution Behavior 执行行为：
 *    ds和table api都提供了执行pipelines的方法，如果请求execute则会编译图，提交一个job到集群执行。
 *    ds和table api基本都通过execute一词触发这种行为，然而这两者却略有不同
 *    StreamExecutionEnvironment通过execute方法把之前构建的图提交成一个job，之前构建的图会清空。
 *    table api通过executeSql或者StatementSet提交sql job
 *    默认情况下table提交的job和ds提交的job不能在一个job提交，会分成多个job提交
 *
      // (1)

      // adds a branch with a printing sink to the StreamExecutionEnvironment
      // 添加一个分支到StreamExecutionEnvironment
      tableEnv.toDataStream(table).print();

      // (2)

      // executes a Table API end-to-end pipeline as a Flink job and prints locally,
      // thus (1) has still not been executed
      // 使用table api提交一个job，这时(1)还没有被执行
      table.execute().print();

      // executes the DataStream API pipeline with the sink defined in (1) as a
      // Flink job, (2) was already running before
      // 使用table api提交一个job， (1)会执行。在这之前，(2)已经执行了。
      env.execute();
 */
class ConvertBetweenDataStreamAndTableSuite extends AnyFunSuite with BeforeAndAfterAll {
  def parallelism: Int = 1

  /**
   * StreamTableEnvironment会继承使用StreamExecutionEnvironment的配置，但是在StreamTableEnvironment实例化修改StreamExecutionEnvironment的配置，StreamTableEnvironment并不一定会生效
   * 所以建议在StreamTableEnvironment实例化之前设置完StreamExecutionEnvironment的配置
   *
   * 参数配置代码示例：
    // create Scala DataStream API

      val env = StreamExecutionEnvironment.getExecutionEnvironment

      // set various configuration early

      env.setMaxParallelism(256)

      env.getConfig.addDefaultKryoSerializer(classOf[MyCustomType], classOf[CustomKryoSerializer])

      env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

      // then switch to Scala Table API

      val tableEnv = StreamTableEnvironment.create(env)

      // set configuration early

      tableEnv.getConfig.setLocalTimeZone(ZoneId.of("Europe/Berlin"))

      // start defining your pipelines in both APIs...
   *
   */
  test("Configuration "){
  }

  test("ConvertBetweenDataStreamAndTable"){
    // create environments of both APIs
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableEnv = StreamTableEnvironment.create(env)

    // create a DataStream
    val dataStream = env.fromElements("Alice", "Bob", "John")


    // interpret the insert-only DataStream as a Table
    // 把一个insert-only的DataStream解释翻译成Table
    val inputTable = tableEnv.fromDataStream(dataStream)

    // register the Table object as a view and query it
    // 注册视图，使之可以在sql中使用
    tableEnv.createTemporaryView("InputTable", inputTable)
    val resultTable = tableEnv.sqlQuery("SELECT UPPER(f0) FROM InputTable")

    // interpret the insert-only Table as a DataStream again
    // 把一个insert-only的Table解释翻译成DataStream
    val resultStream = tableEnv.toDataStream(resultTable)

    // add a printing sink and execute in DataStream API
    resultStream.print()
    env.execute()
  }

}
