package com.flink.connector.test

import com.flink.connector.es.{EsRestClient, EsWriter}
import org.elasticsearch.hadoop.cfg.ConfigurationOptions._
import org.scalatest.funsuite.AnyFunSuite

class EsWriterSuite extends AnyFunSuite{
  test("create_index"){
    val client = new EsRestClient("127.0.0.1:9200", None, None)

    val mappings = """
    {
        "settings" : {
            "number_of_shards" : 2,
            "number_of_replicas" : 0
        },
        "mappings" : {
            "poem_test" : {
                "dynamic" : "strict",
                "_all" : {
                  "enabled" : false
                },
                "properties" : {
                    "title" : { "type" : "text" },
                    "author" : { "type" : "keyword" },
                    "year" : { "type" : "integer" },
                    "content" : { "type" : "text" }
                }
            }
        }
    }
    """
    val rst = client.put("/poetry_test?pretty", mappings){ case (code, rst) =>
      println("code", code)
      rst
    }

    println(rst)
  }

  test("clear_index"){
    val client = new EsRestClient("127.0.0.1:9200", None, None)

    val data = """
    {
        "query": {
            "match_all": {}
        }
    }
    """
    val rst = client.post("/poetry_test/poem_test/_delete_by_query?conflicts=proceed&pretty", data){ case (code, rst) =>
      println("code", code)
      rst
    }

    println(rst)
  }


  /**
   * create必须传入id，id存在会报错。
   * 所以create用的不多
   */
  test("create"){
    val cfg = Map(
      ES_RESOURCE_WRITE -> "poetry_test/poem_test",
      ES_WRITE_OPERATION -> "create",
      ES_MAPPING_ID -> "_id"
    )
    val esWriter = new EsWriter[Map[String, Any]](cfg)
    esWriter.init()

    val datas = Seq(
      Map("_id" -> "12", "title" -> "赠妓云英", "author" -> "罗隐33", "year" -> 1300, "content" -> "钟陵醉别十余春，重见云英掌上身。我未成名卿未嫁，可能俱是不如人。"),
      Map("_id" -> "11", "title" -> "过故洛阳城", "author" -> "司马光33", "year" -> 1200, "content" -> "烟愁雨啸黍华生，宫阙簪裳旧帝京。若问古今兴废事，请君只看洛阳城。"),
      Map("_id" -> "13", "title" -> "芙蓉楼送辛渐", "author" -> "王昌龄33", "year" -> 1500, "content" -> "寒雨连江夜入吴，平明送客楚山孤。洛阳亲友如相问，一片冰心在玉壶。")
    )

    /**
     * poetry_test/poem_test/_bulk
     *
     * debugger查看发送的body
     * [[org.elasticsearch.hadoop.rest.RestRepository#doWriteToIndex]] payload属性就是body
     * [[org.elasticsearch.hadoop.rest.bulk.BulkProcessor#tryFlush]] data属性就是body
     *
     * {"create":{"_id":"12"}}
     * {"author":"罗隐33","year":1300,"content":"钟陵醉别十余春，重见云英掌上身。我未成名卿未嫁，可能俱是不如人。","title":"赠妓云英"}
     * {"create":{"_id":"11"}}
     * {"author":"司马光33","year":1200,"content":"烟愁雨啸黍华生，宫阙簪裳旧帝京。若问古今兴废事，请君只看洛阳城。","title":"过故洛阳城"}
     * {"create":{"_id":"13"}}
     * {"author":"王昌龄33","year":1500,"content":"寒雨连江夜入吴，平明送客楚山孤。洛阳亲友如相问，一片冰心在玉壶。","title":"芙蓉楼送辛渐"}
     *
     */
    for (data <- datas) {
      esWriter.write(data)
    }

    esWriter.flush()
    esWriter.close()
  }


  /**
   * index:id不存在则插入，存在则替换(替换整个文档)。
   */
  test("index1"){
    val cfg = Map(
      ES_RESOURCE_WRITE -> "poetry_test/poem_test",
      ES_WRITE_OPERATION -> "index",
      ES_MAPPING_ID -> "_id"
    )
    val esWriter = new EsWriter[Map[String, Any]](cfg)
    esWriter.init()

    val datas = Seq(
      Map("_id" -> "12", "title" -> "赠妓云英", "author" -> "罗隐", "year" -> 1300, "content" -> "钟陵醉别十余春，重见云英掌上身。我未成名卿未嫁，可能俱是不如人。"),
      Map("_id" -> "11", "title" -> "过故洛阳城", "author" -> "司马光", "year" -> 1200, "content" -> "烟愁雨啸黍华生，宫阙簪裳旧帝京。若问古今兴废事，请君只看洛阳城。"),
      Map("_id" -> "13", "title" -> "芙蓉楼送辛渐", "author" -> "王昌龄", "year" -> 1500, "content" -> "寒雨连江夜入吴，平明送客楚山孤。洛阳亲友如相问，一片冰心在玉壶。"),
      Map("_id" -> "14", "title" -> "乌衣巷", "author" -> "刘禹锡", "year" -> 1200, "content" -> "朱雀桥边野草花，乌衣巷口夕阳斜。旧时王谢堂前燕，飞入寻常百姓家。")
    )

    /**
     * poetry_test/poem_test/_bulk
     *
     * debugger查看发送的body
     * [[org.elasticsearch.hadoop.rest.RestRepository#doWriteToIndex]] payload属性就是body
     * [[org.elasticsearch.hadoop.rest.bulk.BulkProcessor#tryFlush]] data属性就是body
     *
     * {"index":{"_id":"12"}}
     * {"author":"罗隐","year":1300,"content":"钟陵醉别十余春，重见云英掌上身。我未成名卿未嫁，可能俱是不如人。","title":"赠妓云英"}
     * {"index":{"_id":"11"}}
     * {"author":"司马光","year":1200,"content":"烟愁雨啸黍华生，宫阙簪裳旧帝京。若问古今兴废事，请君只看洛阳城。","title":"过故洛阳城"}
     * {"index":{"_id":"13"}}
     * {"author":"王昌龄","year":1500,"content":"寒雨连江夜入吴，平明送客楚山孤。洛阳亲友如相问，一片冰心在玉壶。","title":"芙蓉楼送辛渐"}
     * {"index":{"_id":"14"}}
     * {"author":"刘禹锡","year":1200,"content":"朱雀桥边野草花，乌衣巷口夕阳斜。旧时王谢堂前燕，飞入寻常百姓家。","title":"乌衣巷"}
     *
     */
    for (data <- datas) {
      esWriter.write(data)
    }

    esWriter.flush()
    esWriter.close()
  }

  /**
   * index:不传id，es自动生成随机id。
   * 不设置ES_MAPPING_ID，全部都产生随机id。
   */
  test("index2"){
    val cfg = Map(
      ES_RESOURCE_WRITE -> "poetry_test/poem_test",
      ES_WRITE_OPERATION -> "index"
      //ES_MAPPING_ID -> "_id"
    )
    val esWriter = new EsWriter[Map[String, Any]](cfg)
    esWriter.init()

    val datas = Seq(
      Map("title" -> "赠妓云英", "author" -> "罗隐", "year" -> 1300, "content" -> "钟陵醉别十余春，重见云英掌上身。我未成名卿未嫁，可能俱是不如人。"),
      Map("title" -> "过故洛阳城", "author" -> "司马光", "year" -> 1200, "content" -> "烟愁雨啸黍华生，宫阙簪裳旧帝京。若问古今兴废事，请君只看洛阳城。"),
      Map("title" -> "芙蓉楼送辛渐", "author" -> "王昌龄", "year" -> 1500, "content" -> "寒雨连江夜入吴，平明送客楚山孤。洛阳亲友如相问，一片冰心在玉壶。"),
      Map("title" -> "乌衣巷", "author" -> "刘禹锡", "year" -> 1200, "content" -> "朱雀桥边野草花，乌衣巷口夕阳斜。旧时王谢堂前燕，飞入寻常百姓家。")
    )

    /**
     * poetry_test/poem_test/_bulk
     *
     * debugger查看发送的body
     * [[org.elasticsearch.hadoop.rest.RestRepository#doWriteToIndex]] payload属性就是body
     * [[org.elasticsearch.hadoop.rest.bulk.BulkProcessor#tryFlush]] data属性就是body
     *
     * {"index":{}}
     * {"title":"赠妓云英","author":"罗隐","year":1300,"content":"钟陵醉别十余春，重见云英掌上身。我未成名卿未嫁，可能俱是不如人。"}
     * {"index":{}}
     * {"title":"过故洛阳城","author":"司马光","year":1200,"content":"烟愁雨啸黍华生，宫阙簪裳旧帝京。若问古今兴废事，请君只看洛阳城。"}
     * {"index":{}}
     * {"title":"芙蓉楼送辛渐","author":"王昌龄","year":1500,"content":"寒雨连江夜入吴，平明送客楚山孤。洛阳亲友如相问，一片冰心在玉壶。"}
     * {"index":{}}
     * {"title":"乌衣巷","author":"刘禹锡","year":1200,"content":"朱雀桥边野草花，乌衣巷口夕阳斜。旧时王谢堂前燕，飞入寻常百姓家。"}
     *
     */
    for (data <- datas) {
      esWriter.write(data)
    }

    esWriter.flush()
    esWriter.close()
  }

  /**
   * index:不传id，es自动生成随机id。
   * 设置ES_MAPPING_ID，ES_MAPPING_ID的值都设置为null。_id为null的产生随机id。
   */
  test("index3"){
    val cfg = Map(
      ES_RESOURCE_WRITE -> "poetry_test/poem_test",
      ES_WRITE_OPERATION -> "index",
      ES_MAPPING_ID -> "_id"
    )
    val esWriter = new EsWriter[Map[String, Any]](cfg)
    esWriter.init()

    val datas = Seq(
      Map("_id" -> null, "title" -> "赠妓云英", "author" -> "罗隐", "year" -> 1300, "content" -> "钟陵醉别十余春，重见云英掌上身。我未成名卿未嫁，可能俱是不如人。"),
      Map("_id" -> null, "title" -> "过故洛阳城", "author" -> "司马光", "year" -> 1200, "content" -> "烟愁雨啸黍华生，宫阙簪裳旧帝京。若问古今兴废事，请君只看洛阳城。"),
      Map("_id" -> null, "title" -> "芙蓉楼送辛渐", "author" -> "王昌龄", "year" -> 1500, "content" -> "寒雨连江夜入吴，平明送客楚山孤。洛阳亲友如相问，一片冰心在玉壶。"),
      Map("_id" -> null, "title" -> "乌衣巷", "author" -> "刘禹锡", "year" -> 1200, "content" -> "朱雀桥边野草花，乌衣巷口夕阳斜。旧时王谢堂前燕，飞入寻常百姓家。")
    )

    /**
     * poetry_test/poem_test/_bulk
     *
     * debugger查看发送的body
     * [[org.elasticsearch.hadoop.rest.RestRepository#doWriteToIndex]] payload属性就是body
     * [[org.elasticsearch.hadoop.rest.bulk.BulkProcessor#tryFlush]] data属性就是body
     *
     * {"index":{"_id":null}}
     * {"author":"罗隐","year":1300,"content":"钟陵醉别十余春，重见云英掌上身。我未成名卿未嫁，可能俱是不如人。","title":"赠妓云英"}
     * {"index":{"_id":null}}
     * {"author":"司马光","year":1200,"content":"烟愁雨啸黍华生，宫阙簪裳旧帝京。若问古今兴废事，请君只看洛阳城。","title":"过故洛阳城"}
     * {"index":{"_id":null}}
     * {"author":"王昌龄","year":1500,"content":"寒雨连江夜入吴，平明送客楚山孤。洛阳亲友如相问，一片冰心在玉壶。","title":"芙蓉楼送辛渐"}
     * {"index":{"_id":null}}
     * {"author":"刘禹锡","year":1200,"content":"朱雀桥边野草花，乌衣巷口夕阳斜。旧时王谢堂前燕，飞入寻常百姓家。","title":"乌衣巷"}
     *
     */
    for (data <- datas) {
      esWriter.write(data)
    }

    esWriter.flush()
    esWriter.close()
  }

  /**
   * update:更新指定的字段，更新不存在的id会报错。
   */
  test("update"){
    val cfg = Map(
      ES_RESOURCE_WRITE -> "poetry_test/poem_test",
      ES_WRITE_OPERATION -> "update",
      ES_MAPPING_ID -> "_id",
      // 发生异常打印日志，放弃重试。org.elasticsearch.hadoop.handler.impl.AbortOnFailure
      "es.write.rest.error.handlers" -> "log",
      "es.write.rest.error.handler.log.logger.name" -> "loges"
    )
    val esWriter = new EsWriter[Map[String, Any]](cfg)
    esWriter.init()

    val datas = Seq(
      Map("_id" -> "12", "title" -> "赠妓云英update"),
      Map("_id" -> "11", "title" -> "过故洛阳城update"),
      Map("_id" -> "13", "title" -> "芙蓉楼送辛渐update"),
      Map("_id" -> "14", "title" -> "乌衣巷update")
    )

    /**
     * poetry_test/poem_test/_bulk
     *
     * debugger查看发送的body
     * [[org.elasticsearch.hadoop.rest.RestRepository#doWriteToIndex]] payload属性就是body
     * [[org.elasticsearch.hadoop.rest.bulk.BulkProcessor#tryFlush]] data属性就是body
     *
     * {"update":{"_id":"12"}}
     * {"doc":{"title":"赠妓云英update"}}
     * {"update":{"_id":"11"}}
     * {"doc":{"title":"过故洛阳城update"}}
     * {"update":{"_id":"13"}}
     * {"doc":{"title":"芙蓉楼送辛渐update"}}
     * {"update":{"_id":"14"}}
     * {"doc":{"title":"乌衣巷update"}}
     *
     */
    for (data <- datas) {
      esWriter.write(data)
    }

    esWriter.flush()
    esWriter.close()
  }


  /**
   * upsert: 和index类似，更新时就是更新部分字段，而不是替换整个文档。
   * 这个可以实现和mysql INSERT ON DUPLICATE KEY UPDATE一样的效果。
   */
  test("upsert"){
    val cfg = Map(
      ES_RESOURCE_WRITE -> "poetry_test/poem_test",
      ES_WRITE_OPERATION -> "upsert",
      ES_MAPPING_ID -> "_id"
    )
    val esWriter = new EsWriter[Map[String, Any]](cfg)
    esWriter.init()

    val datas = Seq(
      Map("_id" -> "12", "title" -> "赠妓云英", "author" -> "罗隐", "year" -> 1300, "content" -> "钟陵醉别十余春，重见云英掌上身。我未成名卿未嫁，可能俱是不如人。"),
      Map("_id" -> "11", "title" -> "过故洛阳城", "author" -> "司马光", "year" -> 1200, "content" -> "烟愁雨啸黍华生，宫阙簪裳旧帝京。若问古今兴废事，请君只看洛阳城。"),
      Map("_id" -> "13", "title" -> "芙蓉楼送辛渐", "author" -> "王昌龄", "year" -> 1500, "content" -> "寒雨连江夜入吴，平明送客楚山孤。洛阳亲友如相问，一片冰心在玉壶。"),
      Map("_id" -> "14", "title" -> "乌衣巷", "author" -> "刘禹锡", "year" -> 1200, "content" -> "朱雀桥边野草花，乌衣巷口夕阳斜。旧时王谢堂前燕，飞入寻常百姓家。")
    )

    /**
     * poetry_test/poem_test/_bulk
     *
     * debugger查看发送的body
     * [[org.elasticsearch.hadoop.rest.RestRepository#doWriteToIndex]] payload属性就是body
     * [[org.elasticsearch.hadoop.rest.bulk.BulkProcessor#tryFlush]] data属性就是body
     *
     * {"update":{"_id":"12"}}
     * {"doc_as_upsert":true,"doc":{"author":"罗隐","year":1300,"content":"钟陵醉别十余春，重见云英掌上身。我未成名卿未嫁，可能俱是不如人。","title":"赠妓云英"}}
     * {"update":{"_id":"11"}}
     * {"doc_as_upsert":true,"doc":{"author":"司马光","year":1200,"content":"烟愁雨啸黍华生，宫阙簪裳旧帝京。若问古今兴废事，请君只看洛阳城。","title":"过故洛阳城"}}
     * {"update":{"_id":"13"}}
     * {"doc_as_upsert":true,"doc":{"author":"王昌龄","year":1500,"content":"寒雨连江夜入吴，平明送客楚山孤。洛阳亲友如相问，一片冰心在玉壶。","title":"芙蓉楼送辛渐"}}
     * {"update":{"_id":"14"}}
     * {"doc_as_upsert":true,"doc":{"author":"刘禹锡","year":1200,"content":"朱雀桥边野草花，乌衣巷口夕阳斜。旧时王谢堂前燕，飞入寻常百姓家。","title":"乌衣巷"}}
     *
     */
    for (data <- datas) {
      esWriter.write(data)
    }

    esWriter.flush()
    esWriter.close()
  }

  /**
   * update:更新指定的字段，更新不存在的id会报错。
   */
  test("update_script"){
    val cfg = Map(
      ES_RESOURCE_WRITE -> "poetry_test/poem_test",
      ES_WRITE_OPERATION -> "update",
      ES_MAPPING_ID -> "_id",
      ES_UPDATE_SCRIPT_INLINE -> "if (ctx._source.year == null || ctx._source.year < params.year){ctx._source.title = params.title; ctx._source.author = params.author; ctx._source.year = params.year; ctx._source.content = params.content;}",
      ES_UPDATE_SCRIPT_PARAMS -> "title:title,author:author,year:year,content:content",
      ES_UPDATE_SCRIPT_LANG -> "painless",
      // 发生异常打印日志，放弃重试。org.elasticsearch.hadoop.handler.impl.AbortOnFailure
      "es.write.rest.error.handlers" -> "log",
      "es.write.rest.error.handler.log.logger.name" -> "loges"
    )
    val esWriter = new EsWriter[Map[String, Any]](cfg)
    esWriter.init()

    val datas = Seq(
      Map("_id" -> "12", "title" -> "赠妓云英update_script", "author" -> "罗隐", "year" -> 13001, "content" -> "钟陵醉别十余春，重见云英掌上身。我未成名卿未嫁，可能俱是不如人。"),
      Map("_id" -> "11", "title" -> "过故洛阳城update_script", "author" -> "司马光", "year" -> 12001, "content" -> "烟愁雨啸黍华生，宫阙簪裳旧帝京。若问古今兴废事，请君只看洛阳城。"),
      Map("_id" -> "13", "title" -> "芙蓉楼送辛渐update_script", "author" -> "王昌龄", "year" -> 15001, "content" -> "寒雨连江夜入吴，平明送客楚山孤。洛阳亲友如相问，一片冰心在玉壶。"),
      Map("_id" -> "14", "title" -> "乌衣巷update_script", "author" -> "刘禹锡", "year" -> 12001, "content" -> "朱雀桥边野草花，乌衣巷口夕阳斜。旧时王谢堂前燕，飞入寻常百姓家。")
    )

    /**
     * poetry_test/poem_test/_bulk
     *
     * debugger查看发送的body
     * [[org.elasticsearch.hadoop.rest.RestRepository#doWriteToIndex]] payload属性就是body
     * [[org.elasticsearch.hadoop.rest.bulk.BulkProcessor#tryFlush]] data属性就是body
     *
     * {"update":{"_id":"12"}}
     * {"script":{"source":"if (ctx._source.year == null || ctx._source.year < params.year){ctx._source.title = params.title; ctx._source.author = params.author; ctx._source.year = params.year; ctx._source.content = params.content;}","lang":"painless","params":{"title":"赠妓云英update_script","author":"罗隐","year":13001,"content":"钟陵醉别十余春，重见云英掌上身。我未成名卿未嫁，可能俱是不如人。"}}}
     * {"update":{"_id":"11"}}
     * {"script":{"source":"if (ctx._source.year == null || ctx._source.year < params.year){ctx._source.title = params.title; ctx._source.author = params.author; ctx._source.year = params.year; ctx._source.content = params.content;}","lang":"painless","params":{"title":"过故洛阳城update_script","author":"司马光","year":12001,"content":"烟愁雨啸黍华生，宫阙簪裳旧帝京。若问古今兴废事，请君只看洛阳城。"}}}
     * {"update":{"_id":"13"}}
     * {"script":{"source":"if (ctx._source.year == null || ctx._source.year < params.year){ctx._source.title = params.title; ctx._source.author = params.author; ctx._source.year = params.year; ctx._source.content = params.content;}","lang":"painless","params":{"title":"芙蓉楼送辛渐update_script","author":"王昌龄","year":15001,"content":"寒雨连江夜入吴，平明送客楚山孤。洛阳亲友如相问，一片冰心在玉壶。"}}}
     * {"update":{"_id":"14"}}
     * {"script":{"source":"if (ctx._source.year == null || ctx._source.year < params.year){ctx._source.title = params.title; ctx._source.author = params.author; ctx._source.year = params.year; ctx._source.content = params.content;}","lang":"painless","params":{"title":"乌衣巷update_script","author":"刘禹锡","year":12001,"content":"朱雀桥边野草花，乌衣巷口夕阳斜。旧时王谢堂前燕，飞入寻常百姓家。"}}}
     *
     * -- 格式化后看看传的啥
     * {"update":{"_id":"12"}}
     * {
          "script":{
              "source":"if (ctx._source.year == null || ctx._source.year < params.year){ctx._source.title = params.title; ctx._source.author = params.author; ctx._source.year = params.year; ctx._source.content = params.content;}",
              "lang":"painless",
              "params":{
                  "title":"赠妓云英update_script",
                  "author":"罗隐",
                  "year":13001,
                  "content":"钟陵醉别十余春，重见云英掌上身。我未成名卿未嫁，可能俱是不如人。"
              }
          }
      }
     */
    for (data <- datas) {
      esWriter.write(data)
    }

    esWriter.flush()
    esWriter.close()
  }

  /**
   * upsert: 和index类似，更新时就是更新部分字段，而不是替换整个文档。
   * 这个可以实现和mysql INSERT ON DUPLICATE KEY UPDATE一样的效果。
   */
  test("upsert_script"){
    val cfg = Map(
      ES_RESOURCE_WRITE -> "poetry_test/poem_test",
      ES_WRITE_OPERATION -> "upsert",
      ES_MAPPING_ID -> "_id",
      ES_UPDATE_SCRIPT_INLINE -> "if (ctx._source.year == null || ctx._source.year < params.year){ctx._source.title = params.title; ctx._source.author = params.author; ctx._source.year = params.year; ctx._source.content = params.content;}",
      ES_UPDATE_SCRIPT_PARAMS -> "title:title,author:author,year:year,content:content",
      ES_UPDATE_SCRIPT_LANG -> "painless"
    )
    val esWriter = new EsWriter[Map[String, Any]](cfg)
    esWriter.init()

    val datas = Seq(
      Map("_id" -> "12", "title" -> "赠妓云英script", "author" -> "罗隐", "year" -> 13001, "content" -> "钟陵醉别十余春，重见云英掌上身。我未成名卿未嫁，可能俱是不如人。"),
      Map("_id" -> "11", "title" -> "过故洛阳城script", "author" -> "司马光", "year" -> 12001, "content" -> "烟愁雨啸黍华生，宫阙簪裳旧帝京。若问古今兴废事，请君只看洛阳城。"),
      Map("_id" -> "13", "title" -> "芙蓉楼送辛渐script", "author" -> "王昌龄", "year" -> 15001, "content" -> "寒雨连江夜入吴，平明送客楚山孤。洛阳亲友如相问，一片冰心在玉壶。"),
      Map("_id" -> "14", "title" -> "乌衣巷script", "author" -> "刘禹锡", "year" -> 12001, "content" -> "朱雀桥边野草花，乌衣巷口夕阳斜。旧时王谢堂前燕，飞入寻常百姓家。")
    )

    /**
     * poetry_test/poem_test/_bulk
     *
     * debugger查看发送的body
     * [[org.elasticsearch.hadoop.rest.RestRepository#doWriteToIndex]] payload属性就是body
     * [[org.elasticsearch.hadoop.rest.bulk.BulkProcessor#tryFlush]] data属性就是body
     *
     * {"update":{"_id":"12"}}
     * {"script":{"source":"if (ctx._source.year == null || ctx._source.year < params.year){ctx._source.title = params.title; ctx._source.author = params.author; ctx._source.year = params.year; ctx._source.content = params.content;}","lang":"painless","params":{"title":"赠妓云英script","author":"罗隐","year":13001,"content":"钟陵醉别十余春，重见云英掌上身。我未成名卿未嫁，可能俱是不如人。"}},"upsert":{"author":"罗隐","year":13001,"content":"钟陵醉别十余春，重见云英掌上身。我未成名卿未嫁，可能俱是不如人。","title":"赠妓云英script"}}
     * {"update":{"_id":"11"}}
     * {"script":{"source":"if (ctx._source.year == null || ctx._source.year < params.year){ctx._source.title = params.title; ctx._source.author = params.author; ctx._source.year = params.year; ctx._source.content = params.content;}","lang":"painless","params":{"title":"过故洛阳城script","author":"司马光","year":12001,"content":"烟愁雨啸黍华生，宫阙簪裳旧帝京。若问古今兴废事，请君只看洛阳城。"}},"upsert":{"author":"司马光","year":12001,"content":"烟愁雨啸黍华生，宫阙簪裳旧帝京。若问古今兴废事，请君只看洛阳城。","title":"过故洛阳城script"}}
     * {"update":{"_id":"13"}}
     * {"script":{"source":"if (ctx._source.year == null || ctx._source.year < params.year){ctx._source.title = params.title; ctx._source.author = params.author; ctx._source.year = params.year; ctx._source.content = params.content;}","lang":"painless","params":{"title":"芙蓉楼送辛渐script","author":"王昌龄","year":15001,"content":"寒雨连江夜入吴，平明送客楚山孤。洛阳亲友如相问，一片冰心在玉壶。"}},"upsert":{"author":"王昌龄","year":15001,"content":"寒雨连江夜入吴，平明送客楚山孤。洛阳亲友如相问，一片冰心在玉壶。","title":"芙蓉楼送辛渐script"}}
     * {"update":{"_id":"14"}}
     * {"script":{"source":"if (ctx._source.year == null || ctx._source.year < params.year){ctx._source.title = params.title; ctx._source.author = params.author; ctx._source.year = params.year; ctx._source.content = params.content;}","lang":"painless","params":{"title":"乌衣巷script","author":"刘禹锡","year":12001,"content":"朱雀桥边野草花，乌衣巷口夕阳斜。旧时王谢堂前燕，飞入寻常百姓家。"}},"upsert":{"author":"刘禹锡","year":12001,"content":"朱雀桥边野草花，乌衣巷口夕阳斜。旧时王谢堂前燕，飞入寻常百姓家。","title":"乌衣巷script"}}
     *
     * {"update":{"_id":"12"}}
      {
          "script":{
              "source":"if (ctx._source.year == null || ctx._source.year < params.year){ctx._source.title = params.title; ctx._source.author = params.author; ctx._source.year = params.year; ctx._source.content = params.content;}",
              "lang":"painless",
              "params":{
                  "title":"赠妓云英script",
                  "author":"罗隐",
                  "year":13001,
                  "content":"钟陵醉别十余春，重见云英掌上身。我未成名卿未嫁，可能俱是不如人。"
              }
          },
          "upsert":{
              "author":"罗隐",
              "year":13001,
              "content":"钟陵醉别十余春，重见云英掌上身。我未成名卿未嫁，可能俱是不如人。",
              "title":"赠妓云英script"
          }
      }
     */
    for (data <- datas) {
      esWriter.write(data)
    }

    esWriter.flush()
    esWriter.close()
  }

  /**
   * delete:删除指定的id，删除不存在的id会报错。
   * 然而在这个EsWriter中，删除不存在的id没有报错。
   */
  test("delete"){
    val cfg = Map(
      ES_RESOURCE_WRITE -> "poetry_test/poem_test",
      ES_WRITE_OPERATION -> "delete",
      ES_MAPPING_ID -> "_id",
      // 发生异常打印日志，放弃重试。org.elasticsearch.hadoop.handler.impl.AbortOnFailure
      "es.write.rest.error.handlers" -> "log",
      "es.write.rest.error.handler.log.logger.name" -> "loges"
    )
    val esWriter = new EsWriter[Map[String, Any]](cfg)
    esWriter.init()

    val datas = Seq(
      Map("_id" -> "12", "title" -> "赠妓云英"),
      Map("_id" -> "11", "title" -> "过故洛阳城"),
      Map("_id" -> "13", "title" -> "芙蓉楼送辛渐"),
      Map("_id" -> "14", "title" -> "乌衣巷")
    )

    /**
     * poetry_test/poem_test/_bulk
     *
     * debugger查看发送的body
     * [[org.elasticsearch.hadoop.rest.RestRepository#doWriteToIndex]] payload属性就是body
     * [[org.elasticsearch.hadoop.rest.bulk.BulkProcessor#tryFlush]] data属性就是body
     *
     * {"delete":{"_id":"12"}}
     * {"delete":{"_id":"11"}}
     * {"delete":{"_id":"13"}}
     * {"delete":{"_id":"14"}}
     */
    for (data <- datas) {
      esWriter.write(data)
    }

    esWriter.flush()
    esWriter.close()
  }

}
