package com.atguigu.market_analysis

import com.atguigu.bean.{AdClickEvent, AdCountViewByProvince, BlackListWarning}
import com.atguigu.function.{FilterBlackListUser, MyAdCountAgg, MyAdCountByProvinceResult}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object AdCountByProvince {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic( TimeCharacteristic.EventTime )

    // 从文件中读取数据，map成阳历类，并提取时间戳watermark
    val resourc = getClass.getResource("/AdClickLog.csv")
    val dataDStream: DataStream[AdCountViewByProvince] = env
      .readTextFile(resourc.getPath)
      .map( data=>{
        val dataArray = data.split(",")
        AdClickEvent(dataArray(0).toLong, dataArray(1).toLong, dataArray(2), dataArray(3), dataArray(4).toLong)
      } )
      .assignAscendingTimestamps(_.timestamp * 1000L)
      // 自定义一个ProccessFunction 实现刷单行为的检测和过滤
      .keyBy( data=> (data.userId, data.adId)) // 根据用户id和广告id分组，统计count值
      .process( FilterBlackListUser(100) )
      // 做开窗统计，得到聚合结果
      .keyBy( _.province ) // 按照省份分组统计
      .timeWindow( Time.hours(1), Time.seconds(5) )
      .aggregate( MyAdCountAgg(), MyAdCountByProvinceResult() )


    dataDStream.print(" data ")
    dataDStream.getSideOutput( new OutputTag[BlackListWarning]("blacklist")).print(" blacklist ")
    env.execute( "ad count by province job" )
  }
}
