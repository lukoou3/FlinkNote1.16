package com.flink.sql.dsintegration

import java.time.Instant

import com.flink.base.FlinkBaseSuite
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api._
import LegacyConversionSuite._
import org.apache.flink.table.data.RowData
import org.apache.flink.types.Row

/**
 * https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/dev/table/data_stream_api/#legacy-conversion
 * Legacy Conversion 遗留的废弃的转换
 * 在未来版本中会删除的api。特别是，这些部分可能没有很好地集成到最近的许多新特性和重构中（例如，RowKind设置不正确，类型系统集成不顺畅）。
 */
class LegacyConversionSuite extends FlinkBaseSuite{
  override def parallelism: Int = 1

  /**
   * 废弃的方法是可以传入Expression列的方法：
   *    def fromDataStream[T](dataStream: DataStream[T], fields: Expression*): Table
   *    def createTemporaryView[T](path: String, dataStream: DataStream[T], fields: Expression*): Unit
   */
  test("ds转table_fromDataStream"){
    val datas = Seq(
      People(1, "aaa", 20, 90.2, Instant.ofEpochMilli(1000)),
      People(2, "bb", 22, 90.2, Instant.ofEpochMilli(1001)),
      People(3, "cc", 21, 90.2, Instant.ofEpochMilli(1002))
    )

    // scala类的typeinfo要使用scala隐式参数推导，TypeInformation.of(classOf[People])这种方式并不能解析的到
    //val ds = env.fromCollection(datas)(TypeInformation.of(classOf[People]))
    val ds = env.fromCollection(datas)

    val table = tEnv.fromDataStream(ds, 'id, 'name, 'age, 'score, 'event_time, 'proctime.proctime)
    table.printSchema()
    tEnv.createTemporaryView("tmp_tb", table)
    tEnv.createTemporaryView("tmp_tb2", ds, 'id, 'name, 'age, 'score, 'event_time, 'proctime.proctime)

    tEnv.sqlQuery("select * from tmp_tb")
      .execute()
      .print()

  }

  /**
   * toAppendStream的table转ds用CaseClass，不是按照name匹配的，是按照位置匹配的，这个官网也有说明
   *    https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/dev/table/data_stream_api/#convert-a-table-into-a-datastream
   *    Row: fields are mapped by position, arbitrary number of fields, support for null values, no type-safe access.
   *    POJO: fields are mapped by name (POJO fields must be named as Table fields), arbitrary number of fields, support for null values, type-safe access.
   *    Case Class: fields are mapped by position, no support for null values, type-safe access.
   *    Tuple: fields are mapped by position, limitation to 22 (Scala) or 25 (Java) fields, no support for null values, type-safe access.
   *    Atomic Type: Table must have a single field, no support for null values, type-safe access.
   */
  test("table转ds_toAppendStream_DataStream[CaseClassData]"){
    val sql = """
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

    val table = tEnv.sqlQuery("select name,age,cnt from tmp_tb1")
    val ds1: DataStream[CaseClassData1] = table.toAppendStream[CaseClassData1]
    ds1.print()

    /**
     * 报错：
     * Column types of query result and sink for 'null' do not match.
     * Cause: Incompatible types for sink column 'age' at position 0.
     *
     * Query schema: [name: STRING, age: INT, cnt: BIGINT]
     * Sink schema:  [age: INT, name: STRING, cnt: BIGINT]
     */
    //val ds2: DataStream[CaseClassData2] = table.toAppendStream[CaseClassData2]
    //ds2.print()
  }

  test("table转ds_toAppendStream_DataStream[Row]"){
    val sql = """
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

    val table = tEnv.sqlQuery("select name,age,cnt from tmp_tb1")
    val ds: DataStream[Row] = table.toAppendStream[Row]
    ds.print()
  }

  test("table转ds_toAppendStream_DataStream[RowData]"){
    val sql = """
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

    val table = tEnv.sqlQuery("select name,age,cnt from tmp_tb1")
    val ds: DataStream[RowData] = table.toAppendStream[RowData]
    ds.print()
  }

  test("table转ds_toRetractStream_DataStream[(Boolean, Row)]"){
    val sql = """
    CREATE TABLE tmp_tb1 (
      name string,
      age int,
      cnt bigint,
      proctime as proctime()
    ) WITH (
      'connector' = 'faker',
      'fields.name.expression' = '#{regexify ''(莫南|青丝|璇音|流沙){1}''}',
      'fields.age.expression' = '#{number.numberBetween ''0'',''20''}',
      'fields.cnt.expression' = '#{number.numberBetween ''0'',''20000000000''}',
      'rows-per-second' = '1'
    )
    """
    tEnv.executeSql(sql)

    val table = tEnv.sqlQuery("select name, count(age) age from tmp_tb1 group by name")

    val ds: DataStream[(Boolean, Row)] = table.toRetractStream[Row]
    ds.print()
  }

}

object LegacyConversionSuite{
  case class People(id: Long, name: String, age: Int, score: Double, event_time: Instant)
  case class CaseClassData1(name: String, age: Int, cnt: Long)
  case class CaseClassData2(age: Int, name: String, cnt: Long)

}
