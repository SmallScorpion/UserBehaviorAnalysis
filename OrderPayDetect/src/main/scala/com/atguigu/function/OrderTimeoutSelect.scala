package com.atguigu.function

import java.util

import com.atguigu.bean.{OrderEvent, OrderResult}
import org.apache.flink.cep.PatternTimeoutFunction

// 自定义的PatternTimeoutFunctions
case class OrderTimeoutSelect() extends PatternTimeoutFunction[OrderEvent, OrderResult]{
  override def timeout(map: util.Map[String, util.List[OrderEvent]], l: Long): OrderResult = {
    println(l)
    val timeoutOrderId = map.get("create").iterator().next().orderId

    OrderResult(timeoutOrderId, "timeout")

  }
}
