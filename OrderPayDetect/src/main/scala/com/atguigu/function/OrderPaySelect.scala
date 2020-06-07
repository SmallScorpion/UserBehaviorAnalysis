package com.atguigu.function

import java.util

import com.atguigu.bean.{OrderEvent, OrderResult}
import org.apache.flink.cep.PatternSelectFunction

// 自定义的PatternSselectFunction
case class OrderPaySelect() extends PatternSelectFunction[OrderEvent, OrderResult]{
  override def select(map: util.Map[String, util.List[OrderEvent]]): OrderResult = {

    val payOrderId = map.get("pay").iterator().next().orderId
    OrderResult(payOrderId, "payed")

  }
}
