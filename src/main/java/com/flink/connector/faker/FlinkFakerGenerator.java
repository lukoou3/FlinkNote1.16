package com.flink.connector.faker;

import net.datafaker.Faker;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

class FlinkFakerGenerator extends RichFlatMapFunction<Long, RowData> {

  private Faker faker;
  private Random rand;

  private String[][] fieldExpressions;
  private Float[] fieldNullRates;
  private Integer[] fieldCollectionLengths;
  private LogicalType[] types;
  private long rowsPerSecond;
  private long sleepPerRow;
  private long soFarThisSecond;
  private long nextReadTime;

  public FlinkFakerGenerator(
      String[][] fieldExpressions,
      Float[] fieldNullRates,
      Integer[] fieldCollectionLengths,
      LogicalType[] types,
      long rowsPerSecond) {
    this.fieldExpressions = fieldExpressions;
    this.fieldNullRates = fieldNullRates;
    this.fieldCollectionLengths = fieldCollectionLengths;
    this.types = types;
    this.rowsPerSecond = rowsPerSecond;
    this.sleepPerRow = sleepPerRow;
  }

  @Override
  public void open(final Configuration parameters) throws Exception {
    super.open(parameters);
    faker = new Faker();
    rand = new Random();

    nextReadTime = System.currentTimeMillis();
    soFarThisSecond = 0;
  }

  @Override
  public void flatMap(Long trigger, Collector<RowData> collector) throws Exception {
    // 生成row
    collector.collect(generateNextRow());
    recordAndMaybeRest();
  }

  private void recordAndMaybeRest() throws InterruptedException {
    soFarThisSecond++;
    if(sleepPerRow > 0){
      Thread.sleep(sleepPerRow);
    }
    if (sleepPerRow <= 0 && soFarThisSecond >= getRowsPerSecondForSubTask()) {
      rest();
    }
  }

  private void rest() throws InterruptedException {
    nextReadTime += 1000;
    long toWaitMs = Math.max(0, nextReadTime - System.currentTimeMillis());
    Thread.sleep(toWaitMs);
    soFarThisSecond = 0;
  }

  @VisibleForTesting
  RowData generateNextRow() {
    GenericRowData row = new GenericRowData(fieldExpressions.length);
    for (int i = 0; i < fieldExpressions.length; i++) {

      float fieldNullRate = fieldNullRates[i];
      if (rand.nextFloat() >= fieldNullRate) {
        List<String> values = new ArrayList<String>();
        for (int j = 0; j < fieldCollectionLengths[i]; j++) {
          for (int k = 0; k < fieldExpressions[i].length; k++) {
            // loop for multiple expressions of one field (like map, row fields)
            values.add(faker.expression(fieldExpressions[i][k]));
          }
        }

        // 和LookupFunction中一样，把生成的string类型的数据转换成flink sql内部类型，设置row
        row.setField(
            i, FakerUtils.stringValueToType(values.toArray(new String[values.size()]), types[i]));
      } else {
        row.setField(i, null);
      }
    }
    return row;
  }

  private long getRowsPerSecondForSubTask() {
    /***
     * 计算每秒生成的记录数
     * numSubtasks = getNumberOfParallelSubtasks获取这个subtask总的并行度
     * indexOfThisSubtask = getIndexOfThisSubtask获取这个subtask的索引
     * rowsPerSecond / numSubtasks = 单个subtask生成记录数的速率
     */
    int numSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();
    int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
    long baseRowsPerSecondPerSubtask = rowsPerSecond / numSubtasks;

    // Always emit at least one record per second per subtask so that each subtasks makes some
    // progress.
    // This ensure that the overall number of rows is correct and checkpointing works reliably.
    return Math.max(
        1,
        rowsPerSecond % numSubtasks > indexOfThisSubtask
            ? baseRowsPerSecondPerSubtask + 1
            : baseRowsPerSecondPerSubtask);
  }
}
