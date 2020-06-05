package com.atguigu.function

import java.util.UUID

import com.atguigu.bean.MarketingUserBehavior
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import scala.util.Random


// 定义一个自定义的模拟测试源
case class SimulatedEventSource() extends RichParallelSourceFunction[MarketingUserBehavior]{
  // 定义是否运行的标识位
  var running = true

  // 定义渠道和用户行为的集合
  val channelSet: Seq[String] = Seq("app-store", "huawei-store", "weibo", "wechat")
  val behaviorSet: Seq[String] = Seq("click", "download", "install", "uninstall")
  val rand: Random = Random

  override def cancel(): Unit = running = false

  override def run(ctx: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {
    // 定义最大的数据数量，用于控制测试规模
    val maxCounts = Long.MaxValue
    var count = 0L

    // 无限循环，随机生成所有数据
    while( running && count < maxCounts ){
      val id = UUID.randomUUID().toString
      val channel = channelSet(rand.nextInt(channelSet.size))
      val behavior = behaviorSet(rand.nextInt(behaviorSet.size))
      val ts = System.currentTimeMillis()

      // 使用ctx发出数据
      ctx.collect( MarketingUserBehavior(id, channel, behavior, ts) )

      count += 1
      Thread.sleep(10L)
    }
  }
}
