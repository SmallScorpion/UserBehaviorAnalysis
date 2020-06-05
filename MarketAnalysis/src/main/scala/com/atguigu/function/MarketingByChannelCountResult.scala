package com.atguigu.function

import java.sql.Timestamp

import com.atguigu.bean.{MarketingByChannelCount, MarketingUserBehavior}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


case class MarketingByChannelCountResult() extends ProcessWindowFunction[MarketingUserBehavior, MarketingByChannelCount, (String, String), TimeWindow]{
  override def process(key: (String, String), context: Context, elements: Iterable[MarketingUserBehavior], out: Collector[MarketingByChannelCount]): Unit = {
    val start = new Timestamp(context.window.getStart).toString
    val end = new Timestamp(context.window.getEnd).toString
    val channel = key._1
    val behavior = key._2
    val count = elements.size
    out.collect( MarketingByChannelCount(start, end, channel, behavior, count) )
  }
}
