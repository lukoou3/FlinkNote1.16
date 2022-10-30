package com.flink.stream.high.state

import com.flink.base.FlinkBaseSuite

/**
 * https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/dev/datastream/fault-tolerance/broadcast_state/
 *
 */
class BroadcastStateSuite extends FlinkBaseSuite{
  override def parallelism: Int = 2

}
