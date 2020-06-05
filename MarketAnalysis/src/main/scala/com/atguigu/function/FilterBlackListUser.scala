package com.atguigu.function

import java.sql.Timestamp

import com.atguigu.bean.{AdClickEvent, BlackListWarning}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector



// 实现自定义ProcessFunction，保存当前用户对广告的点击量count，判断是否超过上限
case class FilterBlackListUser(maxCount: Int) extends KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]{
  // 定义状态，保存当前用户对广告的点击量，当前用户是否已被输出到了侧输出流黑名单，0点清空状态的定时器时间戳
  lazy val countState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count", classOf[Long]))
  lazy val isSentState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-sent", classOf[Boolean]))
  lazy val resetTimerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("reset-timerTs", classOf[Long]))

  override def processElement(value: AdClickEvent, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#Context, out: Collector[AdClickEvent]): Unit = {
    // 获取当前的count值，进行判断
    val curCount = countState.value()
    // 如果是当天的第一次处理，注册一个定时器，第二天0点清除所有状态，重新开始
    if( curCount == 0 ){
      val ts = (ctx.timerService().currentProcessingTime()/(1000*60*60*24) + 1) * (24*60*60*1000) - 8*60*60*1000
      ctx.timerService().registerProcessingTimeTimer(ts)
      resetTimerTsState.update(ts)
    }
    // 如果count已经超过上限，那么就过滤掉；如果没有输出过黑名单信息，那么就报警
    if(curCount >= maxCount){
      if( !isSentState.value() ){
        ctx.output(new OutputTag[BlackListWarning]("blacklist"), BlackListWarning(value.userId, value.adId, "click over " + maxCount + " times today."))
        isSentState.update(true)
      }
      return
    }
    // 如果正常，那么直接输出到主流里做开窗统计
    out.collect( value )
    countState.update(curCount + 1)
  }

  // 定时器触发时，判断是否是resetTimer，清空状态
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#OnTimerContext, out: Collector[AdClickEvent]): Unit = {
    if( timestamp == resetTimerTsState.value() ){
      isSentState.clear()
      countState.clear()
      resetTimerTsState.clear()
    }
  }
}
