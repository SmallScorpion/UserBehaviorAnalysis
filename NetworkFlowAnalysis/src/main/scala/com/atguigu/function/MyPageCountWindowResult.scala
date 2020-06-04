package com.atguigu.function

import com.atguigu.bean.PageViewCount
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * 需求：热门页面统计
 * 作用：获取 key - 窗口结束时间 - count值
 */
case class MyPageCountWindowResult() extends WindowFunction[Long, PageViewCount, String, TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PageViewCount]): Unit = {

    out.collect( PageViewCount(key, window.getEnd, input.iterator.next()) )

  }
}
