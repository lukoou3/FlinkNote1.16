package com.flink.connector.test

import com.flink.base.FlinkBaseSuite
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import com.flink.connector.jdbc.{DataStreamJdbcFunctions, JdbcConnectionOptions, JdbcSinkParams, ProductDataStreamJdbcFunctions, TableFunctions}
import JdbcConnectorSuite._
import com.flink.stream.func.LogMap

class JdbcConnectorSuite extends FlinkBaseSuite {
  override def parallelism: Int = 1

  test("addRowDataBatchIntervalJdbcSink") {
    var sql =
      """
    CREATE TABLE tmp_tb1 (
      code int,
      name string,
      age int,
      birthday string
    ) WITH (
      'connector' = 'faker',
      'fields.code.expression' = '#{number.numberBetween ''0'',''20''}',
      'fields.name.expression' = '#{regexify ''(莫南|青丝|璇音|流沙){1}''}',
      'fields.age.expression' = '#{number.numberBetween ''18'',''22''}',
      'fields.birthday.expression' =  '#{date.past ''5'',''0'',''SECONDS''}',
      'rows-per-second' = '2'
    )
    """
    tEnv.executeSql(sql)

    sql =
      """
    select
        *
    from tmp_tb1
    """
    val table = tEnv.sqlQuery(sql)

    table.addRowDataBatchIntervalJdbcSink(JdbcSinkParams(
      "people",
      JdbcConnectionOptions("jdbc:mysql://localhost:3306/jdbc_test?characterEncoding=utf8", "root",
        "123456", "com.mysql.jdbc.Driver"),
      5, 3000
    ))
  }

  test("addBatchIntervalJdbcSink") {
    env.getConfig.disableObjectReuse()
    var sql = """
    CREATE TABLE tmp_tb1 (
      code int,
      name string,
      age int,
      birthday string
    ) WITH (
      'connector' = 'faker',
      'fields.code.expression' = '#{number.numberBetween ''0'',''20''}',
      'fields.name.expression' = '#{regexify ''(莫南|青丝|璇音|流沙){1}''}',
      'fields.age.expression' = '#{number.numberBetween ''18'',''22''}',
      'fields.birthday.expression' =  '#{date.past ''5'',''0'',''SECONDS''}',
      'rows-per-second' = '2'
    )
    """
    tEnv.executeSql(sql)

    sql =
      """
    select
        *
    from tmp_tb1
    """
    val table = tEnv.sqlQuery(sql)
    table.toDataStream[People](classOf[People]).addBatchIntervalJdbcSink(
      "people",
      JdbcConnectionOptions("jdbc:mysql://localhost:3306/jdbc_test?characterEncoding=utf8", "root",
        "123456", "com.mysql.jdbc.Driver"),
      5, 3000
    )
  }

  test("addKeyedBatchIntervalJdbcSink") {
    env.getConfig.disableObjectReuse()
    var sql =
      """
    CREATE TABLE tmp_tb1 (
      code int,
      name string,
      age int,
      birthday string
    ) WITH (
      'connector' = 'faker',
      'fields.code.expression' = '#{number.numberBetween ''0'',''2''}',
      'fields.name.expression' = '#{regexify ''(莫南|青丝|璇音|流沙){1}''}',
      'fields.age.expression' = '#{number.numberBetween ''18'',''22''}',
      'fields.birthday.expression' =  '#{date.past ''5'',''0'',''SECONDS''}',
      'rows-per-second' = '3'
    )
    """
    tEnv.executeSql(sql)

    sql =
      """
    select
        *
    from tmp_tb1
    """
    val table = tEnv.sqlQuery(sql)
    import JdbcConnectorSuite._
    table.toDataStream[People](classOf[People])
      .map(new LogMap[People]("LogMap"))
      .addKeyedBatchIntervalJdbcSink(_.code)(identity)(
      "people",
      JdbcConnectionOptions("jdbc:mysql://localhost:3306/jdbc_test?characterEncoding=utf8", "root",
        "123456", "com.mysql.jdbc.Driver"),
      5, 5000
    )
  }
}

object JdbcConnectorSuite{
  case class People(
    code: Int,
    name: String,
    age: Int,
    birthday: String
  )
}