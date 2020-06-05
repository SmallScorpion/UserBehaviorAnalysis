package com.atguigu.function

import com.atguigu.bean.AdClickEvent
import org.apache.flink.api.common.functions.AggregateFunction


/**
 * 广告点击统计
 * 实现自定义聚合函数
 *
 */

case class MyAdCountAgg() extends AggregateFunction[AdClickEvent, Long, Long]{
  override def createAccumulator(): Long = 0L

  override def add(value: AdClickEvent, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}
