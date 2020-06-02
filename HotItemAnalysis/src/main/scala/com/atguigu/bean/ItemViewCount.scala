package com.atguigu.bean

/**
 * 中间聚合结果样例类
 *
 * @param itemId 商品ID
 * @param windowEnd 窗口结束时间
 * @param count 商品点击数计数
 */
case class ItemViewCount( itemId: Long, windowEnd: Long, count: Long )
