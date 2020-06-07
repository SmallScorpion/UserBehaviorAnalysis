package com.atguigu.bean

// orderpay输入类型
case class OrderEvent( orderId: Long, eventType: String, txId: String, timestamp: Long)
