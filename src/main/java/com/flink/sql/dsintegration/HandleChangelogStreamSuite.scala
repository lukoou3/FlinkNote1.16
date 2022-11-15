package com.flink.sql.dsintegration

import com.flink.base.FlinkBaseSuite
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.data.RowData
import org.apache.flink.types.{Row, RowKind}

/**
 * 之前的直接通过传入可变参数重命名列名和定义处理/事件时间的方法被标记废弃了
 * 这些方法都被标记废弃了，之后的版本可能会删除：fromDataStream(stream, $"myLong", $"myString")/toAppendStream/toRetractStream
 * 新版本使用这几个方法：fromDataStream/toDataStream, fromChangelogStream/toChangelogStream
 * https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/dev/table/data_stream_api/#examples-for-fromdatastream
 * https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/dev/table/data_stream_api/#examples-for-fromchangelogstream
 * https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/dev/table/data_stream_api/#legacy-conversion
 *
 * Changelog的Stream和table的转化有下面几个函数可以使用：
 *    fromChangelogStream(DataStream)
 *    fromChangelogStream(DataStream, Schema)
 *    fromChangelogStream(DataStream, Schema, ChangelogMode)
 *    toChangelogStream(Table)
 *    toChangelogStream(Table, Schema)
 *    toChangelogStream(Table, Schema, ChangelogMode)
 */
class HandleChangelogStreamSuite extends FlinkBaseSuite{
  override def parallelism: Int = 1

  /**
   * fromElements传入的ds只能是Row，
   * 这么一般也不会用到
   */
  test("ds转table_fromChangelogStream") {
    // interpret the stream as a retract stream

    // create a changelog DataStream
    val dataStream = env.fromElements(
      Row.ofKind(RowKind.INSERT, "Alice", Int.box(12)),
      Row.ofKind(RowKind.INSERT, "Bob", Int.box(5)),
      Row.ofKind(RowKind.UPDATE_BEFORE, "Alice", Int.box(12)),
      Row.ofKind(RowKind.UPDATE_AFTER, "Alice", Int.box(100))
    )(Types.ROW(Types.STRING, Types.INT))


    // interpret the DataStream as a Table
    val table = tEnv.fromChangelogStream(dataStream)

    // register the table under a name and perform an aggregation
    tEnv.createTemporaryView("InputTable", table)
    tEnv.executeSql("SELECT f0 AS name, SUM(f1) AS score FROM InputTable GROUP BY f0")
      .print()
  }

  test("table转ds_toChangelogStream_DataStream[Row]") {
    val sql =
      """
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

    val table = tEnv.sqlQuery("select name, max(age) age from tmp_tb1 group by name")

    /**
     * 编译就直接报错了
     * doesn't support consuming update changes which is produced by node GroupAggregate(groupBy=[name], select=[name, MAX(age) AS age])
     * 可以使用toDataStream校验table是否有update changes操作
     */
    //val rowDs: DataStream[Row] = table.toDataStream
    val rowDs: DataStream[Row] = table.toChangelogStream
    println(rowDs.dataType)
    table.execute().print()

    rowDs.addSink { row =>
      println(row)
    }
  }

  test("table转ds_toChangelogStream_DataStream[RowData]") {
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

    // api显示返回的就只是Row类型
    val rowDs: DataStream[Row] = table.toChangelogStream
    println(rowDs.dataType)


    /**
     * 不管是getRowDataDataStreamInternal还是toRetractStream[RowData]，返回的RowData都不是标准的RowData
     * 我说官方怎么没提供toChangelogStream的RowData版本
     */
    val rowDataDs: DataStream[RowData] = com.flink.connector.common.Utils.getRowDataDataStreamInternal(table, null)
    println(rowDataDs.dataType)

    /**
     * 做了转换：
     * [[org.apache.flink.table.runtime.operators.sink.OutputConversionOperator]]
     * [[org.apache.flink.table.runtime.connector.sink.DataStructureConverterWrapper]]
     * [[org.apache.flink.table.data.conversion.RowRowConverter]]
     */
    rowDs.addSink { row =>
      println("rowDs", row)
    }

    /**
     * 这个RowData是
     * org.apache.flink.table.data.utils.JoinedRowData
     * 可以通过getString等api正常访问属性，可以看
     * [[org.apache.flink.table.data.utils.JoinedRowData#getString(int)]]
     * 这个转换是[[org.apache.flink.table.data.conversion.IdentityConverter]]
     * 性能损失降到最小
     */
    rowDataDs.addSink { row =>
      println(row)
      row.getString(0)
      row.getLong(1)
      println("rowDataDs", row.getRowKind, row.getString(0), row.getLong(1))
    }
    table.toRetractStream[RowData].addSink { row =>
      println("RetractRowDataDs", row)
    }
  }

}
