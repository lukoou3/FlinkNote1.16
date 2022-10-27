package com.flink.connector.faker;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.source.*;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import static com.flink.connector.faker.FlinkFakerTableSourceFactory.UNLIMITED_ROWS;

public class FlinkFakerTableSource
    implements ScanTableSource, LookupTableSource, SupportsLimitPushDown {

  private String[][] fieldExpressions;
  private Float[] fieldNullRates;
  private Integer[] fieldCollectionLengths;
  private ResolvedSchema schema;
  private final LogicalType[] types;
  private long rowsPerSecond;
  private long numberOfRows;
  private long sleepPerRow;

  public FlinkFakerTableSource(
      String[][] fieldExpressions,
      Float[] fieldNullRates,
      Integer[] fieldCollectionLengths,
      ResolvedSchema schema,
      long rowsPerSecond,
      long numberOfRows,
      long sleepPerRow) {
    this.fieldExpressions = fieldExpressions;
    this.fieldNullRates = fieldNullRates;
    this.fieldCollectionLengths = fieldCollectionLengths;
    this.schema = schema;
    // LogicalType
    types =
        schema.getColumns().stream()
            .filter(column -> column.isPhysical())
            .map(Column::getDataType)
            .map(DataType::getLogicalType)
            .toArray(LogicalType[]::new);
    this.rowsPerSecond = rowsPerSecond;
    this.numberOfRows = numberOfRows;
    this.sleepPerRow = sleepPerRow;
  }

  @Override
  public ChangelogMode getChangelogMode() {
    return ChangelogMode.insertOnly();
  }

  @Override
  public ScanRuntimeProvider getScanRuntimeProvider(final ScanContext scanContext) {
    //boolean isBounded = numberOfRows != UNLIMITED_ROWS;
    boolean isBounded = false;

    return new DataStreamScanProvider() {
      @Override
      public DataStream<RowData> produceDataStream(
              ProviderContext providerContext, StreamExecutionEnvironment env) {

        //long to = isBounded ? numberOfRows : Long.MAX_VALUE;
        long to = numberOfRows != UNLIMITED_ROWS ? numberOfRows : Long.MAX_VALUE;
        DataStreamSource<Long> sequence =
            env.fromSource(
                new NumberSequenceSource(1, to),
                WatermarkStrategy.noWatermarks(),
                "Source Generator");

        return sequence.flatMap(
            new FlinkFakerGenerator(
                fieldExpressions, fieldNullRates, fieldCollectionLengths, types, rowsPerSecond));
      }

      @Override
      public boolean isBounded() {
        return isBounded;
      }
    };
  }

  @Override
  public DynamicTableSource copy() {
    return new FlinkFakerTableSource(
        fieldExpressions,
        fieldNullRates,
        fieldCollectionLengths,
        schema,
        rowsPerSecond,
        numberOfRows,
        sleepPerRow
    );
  }

  @Override
  public String asSummaryString() {
    return "FlinkFakerSource";
  }

  @Override
  public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
    /**
     * context.getKeys()返回 int[][]，代表关联key的位置, [最外部列的位置, row类型关联内部属性的位置]
     * 大多数情况下不支持row的属性进行关联, 这种情况keys实际就是一维数组
     *    例如表的类型 a int, b int, c int, d int
     *    关联的条件是a, 则keys = [[0]]
     *    关联的条件是a和b, 则keys = [[0], [1]]
     *
     *    例如表的类型 i INT, s STRING, r ROW < i2 INT, s2 STRING >
     *    关联的条件是i和s2, 则keys = [[0], [2, 1]]
     *
     */
    return TableFunctionProvider.of(
        new FlinkFakerLookupFunction(
            fieldExpressions, fieldNullRates, fieldCollectionLengths, types, context.getKeys()));
  }

  /**
   * 支持limit下推
   * @param limit
   */
  @Override
  public void applyLimit(long limit) {
    this.numberOfRows = limit;
  }
}
