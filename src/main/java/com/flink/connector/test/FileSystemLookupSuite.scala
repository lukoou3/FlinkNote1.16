package com.flink.connector.test

import java.nio.charset.StandardCharsets

import com.flink.base.FlinkBaseSuite
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.data.RowData

import com.flink.sql.utils.TableImplicits._

class FileSystemLookupSuite extends FlinkBaseSuite{
  override def parallelism: Int = 1

  test("file_lookup_json"){
    var sql = """
    CREATE TABLE tmp_tb (
      name string,
      province_id bigint,
      proctime as proctime()
    ) WITH (
      'connector' = 'faker',
      'fields.name.expression' = '#{superhero.name}',
      'fields.province_id.expression' = '#{number.numberBetween ''0'',''40''}',
      'rows-per-second' = '1'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    CREATE TABLE dim_province(
      province_id bigint,
      province_name string
    ) WITH (
      'connector' = 'myfilesystem',
      'path' = 'file:///D:/IdeaWorkspace/FlinkNote1.16/files/province_json.txt',
      'format' = 'json',
      -- format的参数配置，前面需要加format的名称
      'json.fail-on-missing-field' = 'false',
      -- json解析报错会直接返回null(row是null), 没法跳过忽略, {}不会报错, 属性都是null
      'json.ignore-parse-errors' = 'true'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    select
        a.name,
        a.province_id,
        b.province_id dim_province_id,
        b.province_name
    from tmp_tb a
    left join dim_province for system_time as of a.proctime as b on a.province_id = b.province_id
    """
    val table = tEnv.sqlQuery(sql)
    table.printSchema()

    table.execute().print()

    val rowDataDataType = table.getResolvedSchema.toPhysicalRowDataType.bridgedTo(classOf[RowData])
    val rowDataDs = table.toDataStream[RowData](rowDataDataType)

    val serializer = table.getJsonRowDataSerializationSchema
    rowDataDs.addSink{row =>
      println(new String(serializer.serialize(row), StandardCharsets.UTF_8))
    }
  }

  test("file_lookup_orc"){
    var sql = """
    CREATE TABLE tmp_tb (
      name string,
      province_id bigint,
      proctime as proctime()
    ) WITH (
      'connector' = 'faker',
      'fields.name.expression' = '#{superhero.name}',
      'fields.province_id.expression' = '#{number.numberBetween ''0'',''40''}',
      'rows-per-second' = '1'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    CREATE TABLE dim_province(
      province_id bigint,
      -- 可以使用计算列
      -- province_name as cast(province_id as c)
      province_name string
    ) WITH (
      'connector' = 'myfilesystem',
      'path' = 'file:///D:/IdeaWorkspace/FlinkNote1.16/files/dim_common_province_a.orc',
      -- 'path' = 'file:///D:/ChromeDownload/orc',
      'lookup.cache.ttl' = '1 min',
      'format' = 'orc'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    select
        a.name,
        a.province_id,
        b.province_id dim_province_id,
        b.province_name
    from tmp_tb a
    left join dim_province for system_time as of a.proctime as b on a.province_id = b.province_id
    """
    val table = tEnv.sqlQuery(sql)
    table.printSchema()

    table.execute().print()

    val rowDataDataType = table.getResolvedSchema.toPhysicalRowDataType.bridgedTo(classOf[RowData])
    val rowDataDs = table.toDataStream[RowData](rowDataDataType)

    val serializer = table.getJsonRowDataSerializationSchema
    rowDataDs.addSink{row =>
      println(new String(serializer.serialize(row), StandardCharsets.UTF_8))
    }
  }

}
