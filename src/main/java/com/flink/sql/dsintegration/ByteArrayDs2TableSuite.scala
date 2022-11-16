package com.flink.sql.dsintegration

import com.flink.base.FlinkBaseSuite
import com.flink.connector.localfile.LocalFileSourceFunction
import com.flink.serialization.BinarySchema
import com.flink.sql.utils.TableImplicits.DataStreamTableOps

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._

class ByteArrayDs2TableSuite extends FlinkBaseSuite {
  override def parallelism: Int = 1

  /**
   * 明明可以直接注册sql的source table用json格式化，为啥还要写一个方法把DataStream[Array[Byte]]转成table
   * 使用sql的source table写两个sql查询table，会生成两个分支从头读table，而不是把table复制
   * 使用把DataStream[Array[Byte]]转成table，写两个sql查询table，读的都是一个DataStream[RowData]
   */
  test("json"){
    val ds = env.addSource(new LocalFileSourceFunction[Array[Byte]]("D:\\IdeaWorkspace\\FlinkNote1.16\\files\\online_log.json",
      sleep = 1000, deserializer = new BinarySchema))

    val fields ="""
        dt string,
        bs string,
        report_time string,
        browser_uniq_id string,
        os_plant string,
        page_id string,
        page_name string,
        page_param string,
        item_id bigint,
        item_type int,
        visit_time string
     """
    ds.createTemporaryViewUseJsonFormat(tEnv, "tmp_tb", s"row<$fields>")

    var sql = "select * from tmp_tb where page_id = 'HotelRN_Detail'"
    tEnv.sqlQuery(sql).toDataStream.addSink{data =>
      println("data1", data)
    }
    sql = "select * from tmp_tb where page_id = 'ThemePark_Scenic'"
    tEnv.sqlQuery(sql).toDataStream.addSink{data =>
      println("data2", data)
    }

  }


}
