package com.flink.connector.test

import com.flink.base.FlinkBaseSuite
import com.flink.connector.es.{TableFunctions, ProductDataStreamEsFunctions, EsSinkParams}

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.elasticsearch.hadoop.cfg.ConfigurationOptions._

import EsConnectorSuite._

class EsConnectorSuite extends FlinkBaseSuite {
  override def parallelism: Int = 1

  test("insert_sql") {
    var sql = """
    CREATE TABLE tmp_tb1 (
      id string,
      name string,
      birthday string,
      proctime as proctime()
    ) WITH (
      'connector' = 'faker',
      'fields.id.expression' = '#{number.numberBetween ''0'',''20''}',
      'fields.name.expression' = '#{regexify ''(莫南|青丝|璇音|流沙){1}''}',
      'fields.birthday.expression' =  '#{date.past ''5'',''0'',''SECONDS''}',
      'rows-per-second' = '2'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    select
        id _id,
        id title,
        name author,
        cast(id as int) `year`,
        birthday content
    from tmp_tb1
    """
    val rstTable = tEnv.sqlQuery(sql)
    rstTable.printSchema()
    println(rstTable.explain())

    sql =
      """
    CREATE TABLE tmp_tb2 (
      `_id` STRING,
      `title` STRING,
      `author` STRING,
      `year` INT,
      `content` STRING
    ) WITH (
      'connector' = 'myes',
      'cluster-name' = 'localhost',
      'resource' = 'index_test2/type_test',
      'sink.batch.size' = '100',
      'es.mapping.id' = '_id'
    )
    """
    tEnv.executeSql(sql)

    sql =
      """
    insert into tmp_tb2
    select
        id _id,
        id title,
        name author,
        cast(id as int) `year`,
        birthday content
    from tmp_tb1
    """
    tEnv.executeSql(sql)

    Thread.sleep(1000 * 60 * 60)
  }

  test("addRowDataBatchIntervalEsSink") {
    var sql = """
    CREATE TABLE tmp_tb1 (
      id string,
      name string,
      birthday string,
      proctime as proctime()
    ) WITH (
      'connector' = 'faker',
      'fields.id.expression' = '#{number.numberBetween ''0'',''20''}',
      'fields.name.expression' = '#{regexify ''(莫南|青丝|璇音|流沙){1}''}',
      'fields.birthday.expression' =  '#{date.past ''5'',''0'',''SECONDS''}',
      'rows-per-second' = '2'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    select
        id _id,
        id title,
        name author,
        cast(id as int) `year`,
        cast(id as int) `year_two`,
        birthday content
    from tmp_tb1
    """
    val rstTable = tEnv.sqlQuery(sql)

    rstTable.addRowDataBatchIntervalEsSink(EsSinkParams(Map(
      ES_RESOURCE_WRITE -> "index_test30/type_test",
      //ES_INDEX_AUTO_CREATE -> "true",
      ES_MAPPING_ID -> "_id"
    ), 10, 5000))

  }

  test("addRowDataBatchIntervalEsSink-UpsertScript") {
    var sql = """
    CREATE TABLE tmp_tb1 (
      id string,
      name string,
      v int,
      proctime as proctime()
    ) WITH (
      'connector' = 'faker',
      'fields.id.expression' = '#{number.numberBetween ''1'',''6''}',
      'fields.v.expression' = '#{number.numberBetween ''10'',''15''}',
      'fields.name.expression' = '#{regexify ''(莫南|青丝|璇音|流沙){1}bb''}',
      'rows-per-second' = '2'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    select
        id _id,
        id title,
        name author,
        v,
        cast(id as int) `year`,
        concat(id, '_', name) content
    from tmp_tb1
    """
    val rstTable = tEnv.sqlQuery(sql)

    rstTable.addRowDataBatchIntervalEsSink(EsSinkParams(Map(
      ES_RESOURCE_WRITE -> "index_test6/type_test",
      ES_INDEX_AUTO_CREATE -> "true",
      ES_MAPPING_ID -> "_id",
      ES_WRITE_OPERATION -> "upsert",
      ES_UPDATE_SCRIPT_INLINE -> "if (ctx._source.v == null || ctx._source.v < params.v){ctx._source.title = params.title; ctx._source.author = params.author; ctx._source.v = params.v;}",
      ES_UPDATE_SCRIPT_PARAMS -> "title:title,author:author,v:v",
      ES_UPDATE_SCRIPT_LANG -> "painless"
    ), 10, 5000))
  }

  test("addRowDataBatchIntervalEsSink复杂类型"){
    val sql = """
    CREATE TABLE tmp_tb1 (
      id int,
      name string,
      age int,
      cnt bigint,
      data row<name string, age int>,
      names array<string>,
      datas array<row<name string, age int>>,
      log_time timestamp(3),
      log_time_ltz timestamp_ltz(3),
      proctime as proctime()
    ) WITH (
      'connector' = 'faker',
      'fields.id.expression' = '#{number.numberBetween ''0'',''20''}',
      'fields.name.expression' = '#{superhero.name}',
      'fields.name.null-rate' = '0.2',
      'fields.age.expression' = '#{number.numberBetween ''0'',''20''}',
       -- 'fields.age.null-rate' = '0.2',
      'fields.cnt.expression' = '#{number.numberBetween ''0'',''20000000000''}',
      'fields.cnt.null-rate' = '0.2',
      'fields.data.name.expression' = '#{harry_potter.spell}',
      'fields.data.age.expression' = '#{number.numberBetween ''20'',''30''}',
      'fields.data.null-rate' = '0.3',
      'fields.names.expression' = '#{harry_potter.spell}',
      'fields.names.length' = '3',
      'fields.names.null-rate' = '0.3',
      'fields.datas.name.expression' = '#{harry_potter.spell}',
      'fields.datas.age.expression' = '#{number.numberBetween ''20'',''30''}',
      'fields.datas.length' = '3',
      'fields.datas.null-rate' = '0.3',
      'fields.log_time.expression' =  '#{date.past ''5'',''0'',''SECONDS''}',
      'fields.log_time_ltz.expression' =  '#{date.past ''5'',''0'',''SECONDS''}',
      'rows-per-second' = '1'
    )
    """
    tEnv.executeSql(sql)

    val table = tEnv.sqlQuery("select id,name,age,cnt,data,datas from tmp_tb1")

    table.addRowDataBatchIntervalEsSink(EsSinkParams(Map(
      ES_RESOURCE_WRITE -> "index_test5/type_test",
      ES_INDEX_AUTO_CREATE -> "true",
      ES_MAPPING_ID -> "id"
    ), 10, 5000))
  }

  test("addBatchIntervalEsSink"){
    val sql = """
    CREATE TABLE tmp_tb1 (
      id int,
      name string,
      age int,
      cnt bigint,
      data row<name string, age int>,
      names array<string>,
      datas array<row<name string, age int>>,
      log_time timestamp(3),
      log_time_ltz timestamp_ltz(3),
      proctime as proctime()
    ) WITH (
      'connector' = 'faker',
      'fields.id.expression' = '#{number.numberBetween ''0'',''20''}',
      'fields.name.expression' = '#{regexify ''(莫南|青丝|璇音|流沙){1}''}',
      'fields.name.null-rate' = '0.2',
      'fields.age.expression' = '#{number.numberBetween ''0'',''20''}',
       -- 'fields.age.null-rate' = '0.2',
      'fields.cnt.expression' = '#{number.numberBetween ''0'',''20000000000''}',
      'fields.cnt.null-rate' = '0.2',
      'fields.data.name.expression' = '#{regexify ''(莫南|青丝|璇音|流沙){1}''}',
      'fields.data.age.expression' = '#{number.numberBetween ''20'',''30''}',
      'fields.data.null-rate' = '0.3',
      'fields.names.expression' = '#{harry_potter.spell}',
      'fields.names.length' = '3',
      'fields.names.null-rate' = '0.3',
      'fields.datas.name.expression' = '#{harry_potter.spell}',
      'fields.datas.age.expression' = '#{number.numberBetween ''20'',''30''}',
      'fields.datas.length' = '3',
      'fields.datas.null-rate' = '0.3',
      'fields.log_time.expression' =  '#{date.past ''5'',''0'',''SECONDS''}',
      'fields.log_time_ltz.expression' =  '#{date.past ''5'',''0'',''SECONDS''}',
      'rows-per-second' = '1'
    )
    """
    tEnv.executeSql(sql)

    val table = tEnv.sqlQuery("select id,name,age,cnt,data,datas from tmp_tb1")

    val dataDs = table.toDataStream(classOf[TableCaseData])

    // 通过运行时反射获取scala/java字段信息
    dataDs.addBatchIntervalEsSink(Map(
      ES_RESOURCE_WRITE -> "index_test5/type_test",
      //ES_INDEX_AUTO_CREATE -> "true",
      ES_MAPPING_ID -> "id"
    ), 10, 5000)
  }
}

object EsConnectorSuite{
  case class TableCaseSubData(name: String, age: Integer)
  case class TableCaseData(id:Int, name: String, age: Int, cnt: java.lang.Long, data: TableCaseSubData, datas:Array[TableCaseSubData])
}
