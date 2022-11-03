package com.flink.sql.func

case class OnlineLog
(
  var pageId: String = "",
  var userId: String = "",
  var eventTime: Long = 0,
  var time: Long = 0,
  var eventTimeStr: String = "",
  var timeStr: String = "",
  var visitCnt: Int = 0,
  var nullField: String = null
)
