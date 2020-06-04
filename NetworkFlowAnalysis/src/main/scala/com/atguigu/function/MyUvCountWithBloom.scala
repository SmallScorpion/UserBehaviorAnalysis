package com.atguigu.function

import com.atguigu.bean.UvCount
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

/**
 * 布隆过滤器
 */
case class MyUvCountWithBloom() extends ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow]{
  lazy val jedis = new Jedis("hadoop102", 6379)
  // 需要处理1亿用户的去重，定义布隆过滤器大小为大约10亿，取2的整次幂就是2^30
  lazy val bloomFilter = MyBloom(1<<30)

  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {
    // 定义在redis中保存的位图的key，以当前窗口的end作为key，（windowEnd，bitmap）
    val storeKey = context.window.getEnd.toString

    // 把当前uv的count值也保存到redis中，保存成一张叫做count的hash表，（windowEnd，uvcount）
    val countMapKey = "count"

    // 初始化操作，从redis的count表中，查到当前窗口的uvcount值
    var count = 0L
    if (jedis.hget(countMapKey, storeKey) != null) {
      count = jedis.hget(countMapKey, storeKey).toLong
    }

    // 开始做去重，首先拿到userId
    val userId = elements.last._2.toString
    // 调用布隆过滤器的hash函数，计算位图中的偏移量
    val offset = bloomFilter.hash(userId, 61)

    // 使用redis命令，查询位图中对应位置是否为1
    val isExist: Boolean = jedis.getbit(storeKey, offset)
    if (!isExist) {
      // 如果不存在userId，对应位图位置要置1，count加一
      jedis.setbit(storeKey, offset, true)
      jedis.hset(countMapKey, storeKey, (count + 1).toString)
    }
  }
}
