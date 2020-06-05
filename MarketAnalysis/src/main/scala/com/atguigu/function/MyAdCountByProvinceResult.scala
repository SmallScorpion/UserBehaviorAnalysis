package com.atguigu.function

import java.sql.Timestamp

import com.atguigu.bean.AdCountViewByProvince
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * 广告点击量统计
 * 自定义窗口函数，提取窗口信息，，包装成样例类
 */
case class MyAdCountByProvinceResult() extends WindowFunction[Long, AdCountViewByProvince, String, TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[AdCountViewByProvince]): Unit = {
    val windowEnd = new Timestamp( window.getEnd ).toString()
    out.collect( AdCountViewByProvince(windowEnd, key, input.iterator.next()) )
  }
}






































