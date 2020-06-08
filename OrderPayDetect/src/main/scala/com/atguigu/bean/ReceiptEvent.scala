package com.atguigu.bean

// 定义到账事件的样例类
case class ReceiptEvent(txId: String, payChannel: String, timestamp: Long)
