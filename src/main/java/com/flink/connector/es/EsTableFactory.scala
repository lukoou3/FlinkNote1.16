package com.flink.connector.es

import java.time.Duration
import java.util

import org.apache.flink.configuration.{ConfigOption, ConfigOptions, ReadableConfig}
import org.apache.flink.table.connector.sink.DynamicTableSink
import org.apache.flink.table.connector.source.DynamicTableSource
import org.apache.flink.table.factories.{DynamicTableFactory, DynamicTableSinkFactory, DynamicTableSourceFactory, FactoryUtil}
import org.apache.flink.table.utils.TableSchemaUtils
import org.elasticsearch.hadoop.cfg.ConfigurationOptions.{ES_MAPPING_EXCLUDE, ES_NET_HTTP_AUTH_PASS, ES_NET_HTTP_AUTH_USER, ES_NODES, ES_RESOURCE_WRITE}

import scala.collection.JavaConverters._
import EsTableFactory._
import com.flink.connector.jdbc.JdbcTableFactory.SINK_KEYED_MODE_KEYS
import com.flink.connector.common.Utils.StringCfgOps

class EsTableFactory extends DynamicTableSourceFactory with DynamicTableSinkFactory {
  def factoryIdentifier(): String = "myes"

  def createDynamicTableSource(context: DynamicTableFactory.Context): DynamicTableSource = {
    val helper = FactoryUtil.createTableFactoryHelper(this, context)
    val config = helper.getOptions

    validateRequiredOptions(config)

    val physicalSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable.getSchema)

    new EsTableSource(config.get(CLUSTER_NAME), config.get(RESOURCE), physicalSchema)
  }

  def createDynamicTableSink(context: DynamicTableFactory.Context): DynamicTableSink = {
    val helper = FactoryUtil.createTableFactoryHelper(this, context)
    val config = helper.getOptions

    validateRequiredOptions(config)

    val cfg = context.getCatalogTable().getOptions().asScala.filter(_._1.startsWith("es.")).toMap

    val clusterName = config.get(CLUSTER_NAME)
    val resource = config.get(RESOURCE)
    val (nodes, user, password) = esParmas(clusterName)
    val rstCfg = Map[String, String](
      ES_NODES -> nodes,
      ES_RESOURCE_WRITE -> resource,
      ES_MAPPING_EXCLUDE -> "_id"
    ) ++ {
      if (user.nonEmpty) Map(ES_NET_HTTP_AUTH_USER -> user.get, ES_NET_HTTP_AUTH_PASS -> password.get) else Map.empty
    } ++ cfg

    new EsTableSink(
      context.getCatalogTable.getResolvedSchema,
      EsSinkParams(
        rstCfg,
        config.get(SINK_BATCH_SIZE),
        config.get(SINK_BATCH_INTERVAL).toMillis,
        keyedMode = config.get(SINK_KEYED_MODE),
        keys = config.get(SINK_KEYED_MODE_KEYS).toSinkKeyedModeKeys,
        orderBy = config.get(SINK_KEYED_MODE_ORDERBY).toSinkKeyedModeOrderBy,
        updateScriptOrderBy = config.get(SINK_KEYED_MODE_SCRIPT_ORDERBY).toSinkKeyedModeOrderBy
      )
    )
  }

  def validateRequiredOptions(config: ReadableConfig): Unit = {
    for (option <- requiredOptions().asScala) {
      assert(config.get(option) != null, option.key() + "参数必须设置")
    }
  }

  override def requiredOptions(): util.Set[ConfigOption[_]] = {
    val requiredOptions = new util.HashSet[ConfigOption[_]]
    requiredOptions.add(CLUSTER_NAME)
    requiredOptions.add(RESOURCE)
    requiredOptions
  }

  override def optionalOptions(): util.Set[ConfigOption[_]] = {
    val optionalOptions = new util.HashSet[ConfigOption[_]]
    optionalOptions
  }
}

object EsTableFactory {
  val CLUSTER_NAME = ConfigOptions.key("cluster-name").stringType().noDefaultValue()
  val RESOURCE = ConfigOptions.key("resource").stringType().noDefaultValue()
  val SINK_BATCH_SIZE = ConfigOptions.key("sink.batch.size").intType().defaultValue(200)
  val SINK_BATCH_INTERVAL = ConfigOptions.key("sink.batch.interval").durationType().defaultValue(Duration.ofSeconds(5))
  val SINK_KEYED_MODE = ConfigOptions.key("sink.keyed.mode").booleanType.defaultValue(false)
  val SINK_KEYED_MODE_KEYS = ConfigOptions.key("sink.keyed.mode.keys").stringType().defaultValue("")
  val SINK_KEYED_MODE_ORDERBY = ConfigOptions.key("sink.keyed.mode.orderby").stringType().defaultValue("")
  val SINK_KEYED_MODE_SCRIPT_ORDERBY = ConfigOptions.key("sink.keyed.mode.script.orderby").stringType().defaultValue("")

  val esParmas = Map[String, (String, Option[String], Option[String])](
    "localhost" -> ("localhost", None, None)
  )
}

