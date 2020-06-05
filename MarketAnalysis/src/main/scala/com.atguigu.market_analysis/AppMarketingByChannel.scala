package com.atguigu.market_analysis

import com.atguigu.bean.{MarketingByChannelCount, MarketingUserBehavior}
import com.atguigu.function.{MarketingByChannelCountResult, SimulatedEventSource}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object AppMarketingByChannel {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 从自定义数据源读取数据进行处理
    val dataStream: DataStream[MarketingUserBehavior] = env.addSource( SimulatedEventSource() )
      .assignAscendingTimestamps(_.timestamp)

    // 开窗统计，得到渠道的聚合结果
    val resultStream: DataStream[MarketingByChannelCount] = dataStream
      .filter(_.behavior != "uninstall")     // 过滤掉卸载行为
      //      .keyBy("channel", "behavior")
      .keyBy( data => (data.channel, data.behavior) )    // 以(channel, behavior)二元组作为分组的key
      .timeWindow( Time.hours(1), Time.seconds(5) )
      .process( MarketingByChannelCountResult() )

    resultStream.print()

    env.execute("app marketing by channel job")
  }
}
