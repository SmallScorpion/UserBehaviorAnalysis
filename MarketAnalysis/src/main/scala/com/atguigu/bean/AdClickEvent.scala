package com.atguigu.bean

// 广告点击流量输入类型
case class AdClickEvent( userId: Long, adId: Long, province: String, city: String, timestamp: Long)

