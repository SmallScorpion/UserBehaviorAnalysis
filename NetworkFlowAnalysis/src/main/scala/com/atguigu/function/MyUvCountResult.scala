package com.atguigu.function

import com.atguigu.bean.{UserBehavior, UvCount}
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * uv统计： 自定义窗口函数,去重操作
 */
case class MyUvCountResult() extends AllWindowFunction[UserBehavior, UvCount, TimeWindow]{
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {

    // 用一个集合来保存所有得userId,实现自动去重
    var idSet = Set[Long]()

    // 遍历所有得数据，添加到Set中
    for(ub <- input){
      idSet += ub.userId
    }

    // 包装好样例类型输出
    out.collect( UvCount(window.getEnd, idSet.size) )


  }
}
