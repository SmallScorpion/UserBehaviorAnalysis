package com.atguigu.function

import com.atguigu.bean.UserBehavior
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.functions.KeyedProcessFunction

import scala.util.Random

/**
 * pv统计中解决Key数据倾斜的问题，将key进行随机数处理
 */
case class MyMapper(rom: Int) extends MapFunction[UserBehavior, (String, Long)]{
  override def map(value: UserBehavior): (String, Long) = {

    (Random.nextString(rom),1L)

  }
}
