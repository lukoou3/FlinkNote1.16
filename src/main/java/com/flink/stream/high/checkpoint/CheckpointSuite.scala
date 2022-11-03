package com.flink.stream.high.checkpoint

import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend
import org.apache.flink.runtime.state.storage.{FileSystemCheckpointStorage, JobManagerCheckpointStorage}
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.scalatest.funsuite.AnyFunSuite

/**
 * Checkpoints:https://nightlies.apache.org/flink/flink-docs-release-1.16/zh/docs/ops/state/checkpoints/
 * StateBackend:https://nightlies.apache.org/flink/flink-docs-release-1.16/zh/docs/ops/state/state_backends/#%E8%87%AA%E6%97%A7%E7%89%88%E6%9C%AC%E8%BF%81%E7%A7%BB
 * StateBackend和Checkpoints不是一个东西，从1.13开始设置的方法改了。
 *  从 Flink 1.13 版本开始，社区改进了 state backend 的公开类，进而帮助用户更好理解本地状态存储和 checkpoint 存储的区分。
 *  这个变化并不会影响 state backend 和 checkpointing 过程的运行时实现和机制，仅仅是为了更好地传达设计意图。
 *  用户可以将现有作业迁移到新的 API，同时不会损失原有 state。
 *
 * 保留 Checkpoint:
 *    Checkpoint 在默认的情况下仅用于恢复失败的作业，并不保留，当程序取消时 checkpoint 就会被删除。当然，你可以通过配置来保留 checkpoint，这些被保留的 checkpoint 在作业失败或取消时不会被清除。这样，你就可以使用该 checkpoint 来恢复失败的作业。
 *    onfig.setExternalizedCheckpointCleanup(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
 *    ExternalizedCheckpointCleanup 配置项定义了当作业取消时，对作业 checkpoint 的操作：
 *      ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION：当作业取消时，保留作业的 checkpoint。注意，这种情况下，需要手动清除该作业保留的 checkpoint。
 *      ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION：当作业取消时，删除作业的 checkpoint。仅当作业失败时，作业的 checkpoint 才会被保留。
 *
 */
class CheckpointSuite extends AnyFunSuite{

  /**
   * 旧版本的 MemoryStateBackend 等价于使用 HashMapStateBackend 和 JobManagerCheckpointStorage。
   * 看了下JobManagerCheckpointStorage的构造函数，可以设置maxStateSize，maxStateSize默认是5m，超过5m cp会报错应用会失败
   */
  test("MemoryStateBackend"){
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(new HashMapStateBackend)
    env.getCheckpointConfig.setCheckpointStorage(new JobManagerCheckpointStorage)
    env.getCheckpointConfig.setCheckpointStorage(new JobManagerCheckpointStorage(null, 50 * 1024 * 1024))
  }

  /**
   * 旧版本的 FsStateBackend 等价于使用 HashMapStateBackend 和 FileSystemCheckpointStorage。
   */
  test("FsStateBackend"){
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(new HashMapStateBackend)
    env.getCheckpointConfig.setCheckpointStorage("file:///checkpoint-dir")


    // Advanced FsStateBackend configurations, such as write buffer size
    // can be set by using manually instantiating a FileSystemCheckpointStorage object.
    env.getCheckpointConfig.setCheckpointStorage(new FileSystemCheckpointStorage("file:///checkpoint-dir"))
  }

  /**
   * 旧版本的 RocksDBStateBackend 等价于使用 EmbeddedRocksDBStateBackend 和 FileSystemCheckpointStorage.
   */
  test("RocksDBStateBackend"){
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(new EmbeddedRocksDBStateBackend)
    env.getCheckpointConfig.setCheckpointStorage("file:///checkpoint-dir")


    // If you manually passed FsStateBackend into the RocksDBStateBackend constructor
    // to specify advanced checkpointing configurations such as write buffer size,
    // you can achieve the same results by using manually instantiating a FileSystemCheckpointStorage object.
    env.getCheckpointConfig.setCheckpointStorage(new FileSystemCheckpointStorage("file:///checkpoint-dir"))
  }


  test("保留Checkpoint"){
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(new HashMapStateBackend)
    env.getCheckpointConfig.setCheckpointStorage("file:///checkpoint-dir")
    env.getCheckpointConfig.setExternalizedCheckpointCleanup(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
  }
}
