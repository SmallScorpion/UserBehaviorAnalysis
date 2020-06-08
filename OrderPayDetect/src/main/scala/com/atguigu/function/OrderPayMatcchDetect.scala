package com.atguigu.function

import com.atguigu.bean.{OrderEvent, OrderResult}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector


// OrderTimeoutWithoutCep 按照当前数据的类型 以及之前的状态，判断要做的操作
case class OrderPayMatcchDetect() extends KeyedProcessFunction[Long, OrderEvent, OrderResult] {

  // 定义状态，用来保存create和pay是否已经来过
  lazy val isPayedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-payed", classOf[Boolean]))
  lazy val iscreatedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-created", classOf[Boolean]))
  // 定义状态保存定时器时间戳
  lazy val timerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-ts", classOf[Long]))

  // 定义测输出流标签,用于输出超时订单结果
  val orderTimeoutOutputTag = new OutputTag[OrderResult]("timeout")


  override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {

    /*

    // 迟到数据输出到测输出流
    if(value.timestamp * 1000L < ctx.timerService().currentWatermark()){
      ctx.output( new OutputTag[OrderEvent]("late"), value )
    }

    */

    // 拿状态
    val isPaded = isPayedState.value()
    val iscreated = iscreatedState.value()
    val timerTs = timerTsState.value()

    // 判断当前事件的类型，以及之前的状态，不同得组合，有不同的处理
    // 情况1： 来的视据的create，接下来判断是否pay过
    if (value.eventType == "create") {
      // 请款1.1： 如果已经支付过，匹配成功，输出到主流
      if (isPaded) {
        out.collect(OrderResult(value.orderId, "payed"))
        // 已经输出结果了，清空状态和定时器
        isPayedState.clear()
        iscreatedState.clear()
        timerTsState.clear()
        ctx.timerService().deleteEventTimeTimer( timerTs )
      }
      // 情况1.2： 如果还每支付过，要注册一个定时器，开始等待
      else{
        // 定义时间戳
        val ts = value.timestamp * 1000L + 15 * 60 * 1000L
        ctx.timerService().registerEventTimeTimer(ts)
        // 更新状态
        timerTsState.update(ts)
        iscreatedState.update(true)
      }
    }

    // 情况2： 来的是pay，那么要继续判断是否已经create
    else if( value.eventType == "pay" ){
      // 情况2.1.1： 如果已经pay过，还需要判断create和pay的时间差是否超过15分钟
      if( iscreated ){
        // 如果没有超时，主流正常输出结果
        if( value.timestamp * 1000L <= timerTs ){
          out.collect( OrderResult(value.orderId, "payed") )

        }
        // 2.1.2： 如果已经超时，侧输出流输出超时结果
        else {
          ctx.output( orderTimeoutOutputTag, OrderResult(value.orderId, "payed but already timeout") )
        }
        // 已经输出结果了，清空状态和定时器
        isPayedState.clear()
        iscreatedState.clear()
        timerTsState.clear()
        ctx.timerService().deleteEventTimeTimer( timerTs )
      }
      // 情况2.2： 如果没有create过，可能是create丢失，也可能是乱序，需要等待create
      else {
        // 注册一个当前pay事件时间戳的定时器
        ctx.timerService().registerEventTimeTimer( value.timestamp * 1000L )
        timerTsState.update(value.timestamp * 1000L)
        isPayedState.update(true)
      }
    }



  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {

    if( isPayedState.value() ){
      // 如果isPayed未true，说明一定是pay先来，而且没等到create
      ctx.output( orderTimeoutOutputTag, OrderResult(ctx.getCurrentKey, "payed but not created") )
    } else {
      // 如果isPayed为false 说明一定是create先来，等pay没等到
      ctx.output( orderTimeoutOutputTag, OrderResult(ctx.getCurrentKey, "timeout") )

    }
    // 已经输出结果了，清空状态
    isPayedState.clear()
    iscreatedState.clear()
    timerTsState.clear()
  }
}
