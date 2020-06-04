package com.atguigu.function

import com.atguigu.bean.PvCount
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * pv统计窗口函数
 */
case class MyPvCountWindowResult() extends WindowFunction[Long, PvCount, String, TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PvCount]): Unit = {

    out.collect( PvCount(window.getEnd, input.head) )

  }
}
