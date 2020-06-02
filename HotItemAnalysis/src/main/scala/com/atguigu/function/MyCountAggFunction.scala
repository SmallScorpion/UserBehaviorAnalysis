package com.atguigu.function

import com.atguigu.bean.UserBehavior
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.java.tuple.Tuple

/**
 *  需求：热门商品统计TopN
 *  作用：自定义预聚合函数，每来一个数据就count加1
 *        将count结果进行输出
 */
case class MyCountAggFunction() extends AggregateFunction[UserBehavior, Long, Long]{

  override def createAccumulator(): Long = 0L

  override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}
