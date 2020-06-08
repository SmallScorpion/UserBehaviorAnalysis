package com.atguigu.function

import com.atguigu.bean.{OrderEvent, ReceiptEvent}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

// 自定义CoProcessFunction，用状态保存另一条流已来的数据
class TxPayMatchDetect() extends CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]{
  // 定义状态，用来保存已经来到的pay事件和receipt事件
  lazy val payEventState: ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("pay-event", classOf[OrderEvent]))
  lazy val receiptEventState: ValueState[ReceiptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receipt-event", classOf[ReceiptEvent]))

  // 订单事件流里的数据，每来一个就调用一次processElement1
  override def processElement1(pay: OrderEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    // 订单支付事件来了，要考察当前是否已经来过receipt
    val receipt = receiptEventState.value()
    if( receipt != null ){
      // 1. 如果来过，正常匹配输出到主流
      out.collect( (pay, receipt) )
      // 清空状态
      payEventState.clear()
      receiptEventState.clear()
    } else {
      // 2. 如果receipt还没来，注册定时器等待
      ctx.timerService().registerEventTimeTimer(pay.timestamp * 1000L + 5000L)
      // 更新状态
      payEventState.update(pay)
    }
  }

  override def processElement2(receipt: ReceiptEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    // 到账事件来了，要考察当前是否已经来过pay
    val pay = payEventState.value()
    if( pay != null ){
      // 1. 如果来过，正常匹配输出到主流
      out.collect( (pay, receipt) )
      // 清空状态
      payEventState.clear()
      receiptEventState.clear()
    } else {
      // 2. 如果receipt还没来，注册定时器等待
      ctx.timerService().registerEventTimeTimer(receipt.timestamp * 1000L + 3000L)
      // 更新状态
      receiptEventState.update(receipt)
    }
  }

  // 定时器触发，需要判断状态中的值
  override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    // 判断两个状态，哪个不为空，那么就是另一个没来
    if(payEventState.value() != null)
      ctx.output(new OutputTag[OrderEvent]("unmatched-pays"), payEventState.value())
    if(receiptEventState.value() != null )
      ctx.output(new OutputTag[ReceiptEvent]("unmatched-receipts"), receiptEventState.value())

    // 清空状态
    payEventState.clear()
    receiptEventState.clear()
  }
}