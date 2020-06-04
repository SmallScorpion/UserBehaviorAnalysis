package com.atguigu.bean

/**
 * 热门页面统计TopN输入类型
 * @param url
 * @param windowEnd
 * @param count
 */
case class PageViewCount(url: String, windowEnd: Long, count: Long)
