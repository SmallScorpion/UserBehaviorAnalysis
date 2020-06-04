package com.atguigu.function

import java.sql.Timestamp

import com.atguigu.bean.PageViewCount
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * 实现count结果得排序
 * @param topSize
 */
case class MyTopNHotPage(topSize: Int) extends KeyedProcessFunction[Long, PageViewCount, String]{

  /*

  // 定义列表状态，用来保存当前窗口得所有page得count值
  lazy val pageViewCountListState: ListState[PageViewCount] = getRuntimeContext
    .getListState( new ListStateDescriptor[PageViewCount]("pageview-count", classOf[PageViewCount]) )

*/

  // 改进：定义MapState,用来保存当前窗口所有得page值和count值  有更新操作时直接put
  lazy val pageViewCountMapState: MapState[String, Long] = getRuntimeContext
    .getMapState( new MapStateDescriptor[String, Long]("pageview-count", classOf[String], classOf[Long]) )


  override def processElement(value: PageViewCount, ctx: KeyedProcessFunction[Long, PageViewCount, String]#Context, out: Collector[String]): Unit = {

    // pageViewCountListState.add( value )
    pageViewCountMapState.put( value.url, value.count )

    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)

    // 定义一分钟之后得定时器，用于清除状态
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 60 * 1000L)

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PageViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {

    // 判断时间戳，如果时1分钟后得定时器，直接清空状态
    if(timestamp == ctx.getCurrentKey + 60 * 1000L){
      pageViewCountMapState.clear()
      return
    }

    val allPageViewCounts: ListBuffer[PageViewCount] = ListBuffer()

/*
    val iter = pageViewCountListState.get().iterator()

    while(iter.hasNext())
        allPageViewCounts += iter.next()

    pageViewCountListState.clear()
*/

    val iter = pageViewCountMapState.entries().iterator()
    while(iter.hasNext){
      val entry = iter.next()
      allPageViewCounts += PageViewCount(entry.getKey, timestamp - 1, entry.getValue)
    }




    // 将所有count值排序取前N个
    val sortedPageViewCounts = allPageViewCounts.sortWith( _.count > _.count ).take(topSize)

    // 将排序数据包装成可视化的String，便于打印输出
    val result: StringBuilder = new StringBuilder
    result.append("==================================\n")
    result.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n")
    // 遍历排序结果数组，将每个sortedPageViewCounts的url和count值，以及排名输出
    for(i <- sortedPageViewCounts.indices){
      val currentViewCount = sortedPageViewCounts(i)
      result.append("NO").append(i+1).append(":")
        .append(" 页面url=").append(currentViewCount.url)
        .append(" 访问量=").append(currentViewCount.count)
        .append("\n")
    }

    // 控制输出频率
    Thread.sleep(1000)
    out.collect(result.toString())

  }
}
