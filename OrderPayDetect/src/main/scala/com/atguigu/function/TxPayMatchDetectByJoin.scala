package com.atguigu.function

import com.atguigu.bean.{OrderEvent, ReceiptEvent}
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.util.Collector

// 自定义ProcessJoinFunction
case class TxPayMatchDetectByJoin() extends ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]{
  override def processElement(left: OrderEvent, right: ReceiptEvent, ctx: ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    out.collect((left, right))
  }
}
