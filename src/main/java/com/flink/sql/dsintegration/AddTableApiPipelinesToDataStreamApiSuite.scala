package com.flink.sql.dsintegration

import com.flink.base.FlinkBaseSuite
import org.apache.flink.client.deployment.executors.RemoteExecutor
import org.apache.flink.client.program.MiniClusterClient
import org.apache.flink.configuration.{ConfigConstants, Configuration, DeploymentOptions, JobManagerOptions, RestOptions, TaskManagerOptions}
import org.apache.flink.runtime.minicluster.{MiniCluster, MiniClusterConfiguration}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._

/**
 * 默认table和ds的任务提交时分开的，table转ds后sink不算，
 * sql的insert into和ds的env.execute()分别提交table和ds的job
 * 不过有时会同时使用table和ds api，之前我都是table转ds后sink，其实table的job添加到DataStream API程序中：
 *    val tableFromStream = tableEnv.fromDataStream(dataStream)
 *    statementSet.attachAsDataStream() // attach both pipelines to StreamExecutionEnvironment
 */
class AddTableApiPipelinesToDataStreamApiSuite extends FlinkBaseSuite {
  override def parallelism: Int = 1

  /**
   * 错误的写法
   * 端口绑定报错，要新建一个本地的环境
   * 本地模式只能通过sql或者ds一种方式提交，这是两次任务的提交
   */
  test("executeInserSqlAndDataStream.addSink"){
    var sql = """
    CREATE TABLE tmp_tb1 (
      name string,
      age int,
      cnt bigint,
      proctime as proctime()
    ) WITH (
      'connector' = 'faker',
      'fields.name.expression' = '#{superhero.name}',
      'fields.age.expression' = '#{number.numberBetween ''0'',''20''}',
      'fields.cnt.expression' = '#{number.numberBetween ''0'',''20000000000''}',
      'rows-per-second' = '1'
    )
    """
    tEnv.executeSql(sql)

    sql =
      """
    CREATE TABLE tmp_tb2 (
      name string,
      age int,
      cnt bigint,
      proctime TIMESTAMP_LTZ(3)
    ) WITH (
      'connector' = 'mylog',
      'log-level' = 'warn',
      'format' = 'json'
    )
    """
    tEnv.executeSql(sql)

    println("insert into before")
    tEnv.executeSql("insert into tmp_tb2 select * from tmp_tb1")
    println("insert into after")

    val table = tEnv.sqlQuery("select * from tmp_tb1")
    table.toDataStream.addSink(println(_))
  }

  /**
   * 把sql insert转换到ds的streamGraph中
   */
  test("attachAsDataStream"){
    var sql = """
    CREATE TABLE tmp_tb1 (
      name string,
      age int,
      cnt bigint,
      proctime as proctime()
    ) WITH (
      'connector' = 'faker',
      'fields.name.expression' = '#{superhero.name}',
      'fields.age.expression' = '#{number.numberBetween ''0'',''20''}',
      'fields.cnt.expression' = '#{number.numberBetween ''0'',''20000000000''}',
      'rows-per-second' = '1'
    )
    """
    tEnv.executeSql(sql)

    sql =
      """
    CREATE TABLE tmp_tb2 (
      name string,
      age int,
      cnt bigint,
      proctime TIMESTAMP_LTZ(3)
    ) WITH (
      'connector' = 'mylog',
      'log-level' = 'warn',
      'format' = 'json'
    )
    """
    tEnv.executeSql(sql)

    println("insert into before")
    val statementSet = tEnv.createStatementSet()
    statementSet.addInsertSql("insert into tmp_tb2 select * from tmp_tb1")
    statementSet.attachAsDataStream()
    println("insert into after")

    val table = tEnv.sqlQuery("select * from tmp_tb1")
    table.toDataStream.addSink(println(_))
  }

  /**
   * 模拟集群提交，可以看到提交了两个job，实际中要避免这种操作
   */
  test("TranslateExecuteSqlQueryMiniClusterClient") {
    val config = new Configuration()
    config.set(TaskManagerOptions.NUM_TASK_SLOTS, 2: Integer)
    config.setInteger(JobManagerOptions.PORT, 0)

    val cluster = createLocalCluster(config)
    val port = cluster.getRestAddress.get.getPort

    setJobManagerInfoToConfig(config, "localhost", port)
    config.set(DeploymentOptions.TARGET, RemoteExecutor.NAME)
    config.setBoolean(DeploymentOptions.ATTACHED, true)

    println(s"\nStarting local Flink cluster (host: localhost, port: ${port}).\n")

    val remoteSenv = new org.apache.flink.streaming.api.environment.StreamExecutionEnvironment(config)
    env = new StreamExecutionEnvironment(remoteSenv)
    env.setParallelism(1)
    val settings = EnvironmentSettings.newInstance().inStreamingMode().build()
    tEnv = StreamTableEnvironment.create(env, settings)

    var sql = """
    CREATE TABLE tmp_tb1 (
      name string,
      age int,
      cnt bigint,
      proctime as proctime()
    ) WITH (
      'connector' = 'faker',
      'fields.name.expression' = '#{superhero.name}',
      'fields.age.expression' = '#{number.numberBetween ''0'',''20''}',
      'fields.cnt.expression' = '#{number.numberBetween ''0'',''20000000000''}',
      'rows-per-second' = '1'
    )
    """
    tEnv.executeSql(sql)

    sql =
      """
    CREATE TABLE tmp_tb2 (
      name string,
      age int,
      cnt bigint,
      proctime TIMESTAMP_LTZ(3)
    ) WITH (
      'connector' = 'mylog',
      'log-level' = 'warn',
      'format' = 'json'
    )
    """
    tEnv.executeSql(sql)

    println("insert into before")
    tEnv.executeSql("insert into tmp_tb2 select * from tmp_tb1")
    println("insert into after")

    val table = tEnv.sqlQuery("select * from tmp_tb1")
    table.toDataStream.addSink(println(_))

  }
  private def createLocalCluster(flinkConfig: Configuration) = {

    val numTaskManagers = flinkConfig.getInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, ConfigConstants.DEFAULT_LOCAL_NUMBER_TASK_MANAGER)
    val numSlotsPerTaskManager = flinkConfig.getInteger(TaskManagerOptions.NUM_TASK_SLOTS)

    val miniClusterConfig = new MiniClusterConfiguration.Builder()
      .setConfiguration(flinkConfig)
      .setNumSlotsPerTaskManager(numSlotsPerTaskManager)
      .setNumTaskManagers(numTaskManagers)
      .build()

    val cluster = new MiniCluster(miniClusterConfig)
    cluster.start()
    cluster
  }

  private def setJobManagerInfoToConfig(
    config: Configuration,
    host: String, port: Integer): Unit = {

    config.setString(JobManagerOptions.ADDRESS, host)
    config.setInteger(JobManagerOptions.PORT, port)

    config.setString(RestOptions.ADDRESS, host)
    config.setInteger(RestOptions.PORT, port)
  }
}
