package com.atguigu.function

import com.atguigu.bean.{LoginEvent, LoginFailWarning}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

// 实现自定义的KeyedProcessFunction
case class LoginFailDetectWarning(maxFailTimes: Int) extends KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]{
  // 定义状态，用来保存所有的登录失败事件，以及注册的定时器时间戳
  lazy val loginFailListState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("loginfail-list", classOf[LoginEvent]))
  lazy val timerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-ts", classOf[Long]))

  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]#Context, out: Collector[LoginFailWarning]): Unit = {
    // 每来一个数据，判断当前登录事件是成功还是失败
    if( value.eventType == "fail" ){
      // 如果是失败，保存到ListState里，还需要判断是否应该注册定时器
      loginFailListState.add(value)
      if( timerTsState.value() == 0 ){
        // 如果没有定时器，就注册一个2秒后的
        val ts = value.timestamp * 1000L + 2000L
        ctx.timerService().registerEventTimeTimer(ts)
        timerTsState.update(ts)
      }
    } else {
      // 如果是成功，删除定时器，清空状态，重新开始
      ctx.timerService().deleteEventTimeTimer(timerTsState.value())
      loginFailListState.clear()
      timerTsState.clear()
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]#OnTimerContext, out: Collector[LoginFailWarning]): Unit = {
    // 定时器触发时，说明没有成功事件来，统计所有的失败事件个数，如果大于设定值就报警
    import scala.collection.JavaConversions._
    val loginFailList = loginFailListState.get().toList

    if( loginFailList.length >= maxFailTimes ){
      out.collect( LoginFailWarning(
        ctx.getCurrentKey,
        loginFailList.head.timestamp,
        loginFailList.last.timestamp,
        "login fail in 2s for " + loginFailList.length + " times."
      ) )
    }
    // 清空状态
    loginFailListState.clear()
    timerTsState.clear()
  }
}