package com.atguigu.function

import org.apache.flink.api.common.functions.AggregateFunction

/**
 * pv 统计
 */
case class MyPvCountAgg() extends AggregateFunction[ (String, Long), Long, Long ]{
  override def createAccumulator(): Long = 0L

  override def add(value: (String, Long), accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}
