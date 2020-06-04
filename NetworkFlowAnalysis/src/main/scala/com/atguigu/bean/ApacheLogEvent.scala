package com.atguigu.bean

/**
 * 热门页面统计TopN输出类型
 * @param ip
 * @param userId
 * @param eventTime
 * @param method
 * @param url
 */
case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)
