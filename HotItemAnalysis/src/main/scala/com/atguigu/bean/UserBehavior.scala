package com.atguigu.bean

/**
 * 输入数据样例类
 *
 * @param userId 用户ID
 * @param itemId  商品ID
 * @param categoryId  商品所属类别ID
 * @param behavior 用户行为类型，包括(‘pv’, ‘’buy, ‘cart’, ‘fav’)
 * @param timestamp 行为发生的时间戳，单位秒
 */
case class UserBehavior( userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long )
