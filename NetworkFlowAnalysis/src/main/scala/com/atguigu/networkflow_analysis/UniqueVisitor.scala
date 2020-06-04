package com.atguigu.networkflow_analysis

import com.atguigu.bean.{PvCount, UserBehavior, UvCount}
import com.atguigu.function.{MyMapper, MyPvCountAgg, MyPvCountWindowResult, MyTotalPvCount, MyUvCountResult}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 独立用户访问数
 */
object UniqueVisitor {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic( TimeCharacteristic.EventTime )
    // env.setParallelism(1)
    env.setParallelism(4)

    val dataDStream: DataStream[UvCount] = env
      .readTextFile("D:\\MyWork\\WorkSpaceIDEA\\UserBehaviorAnalysis\\NetworkFlowAnalysis\\src\\main\\resources\\UserBehavior.csv")
      .map(
        data => {
          val dataArray: Array[String] = data.split(",")
          UserBehavior( dataArray(0).toLong, dataArray(1).toLong, dataArray(2).toInt, dataArray(3), dataArray(4).toLong)
        }
      )
      .assignAscendingTimestamps(_.timestamp * 1000L) // 由于数据时间字段是升序可直接使用此方法

      // 进行开窗统计聚合
      .filter( _.behavior == "pv" )
      .timeWindowAll( Time.hours(1) ) // 全窗口函数统计每小时得uv值
      .apply( MyUvCountResult() )

    dataDStream.print(" data ")
    env.execute( "uv job test" )
  }
}
