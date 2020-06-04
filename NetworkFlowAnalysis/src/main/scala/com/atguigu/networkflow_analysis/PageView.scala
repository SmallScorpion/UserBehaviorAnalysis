package com.atguigu.networkflow_analysis

import java.text.SimpleDateFormat

import com.atguigu.bean.{ApacheLogEvent, PvCount, UserBehavior}
import com.atguigu.function.{MyMapper, MyPvCountAgg, MyPvCountWindowResult, MyTotalPvCount}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * pv 统计
 */
object PageView {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic( TimeCharacteristic.EventTime )
    // env.setParallelism(1)
    env.setParallelism(4)

    val dataDStream: DataStream[PvCount] = env
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
      // .map( data => ("pv",1L) ) // map层二元组，用一个哑key来作为分组得key
      .map( MyMapper(4) ) // 解决只有一个key得数据倾斜问题
      .keyBy(_._1)
      .timeWindow( Time.hours(1) ) // 统计每小时得pv值
      .aggregate( MyPvCountAgg(), MyPvCountWindowResult() )
      // 把每个key对应的 pv count值合并
      .keyBy(_.windowEnd)
      .process( MyTotalPvCount() )


    dataDStream.print(" data ")
    env.execute( "pv job test" )
  }
}
