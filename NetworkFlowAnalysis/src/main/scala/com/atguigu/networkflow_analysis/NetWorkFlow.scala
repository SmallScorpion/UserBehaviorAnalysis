package com.atguigu.networkflow_analysis

import java.text.SimpleDateFormat

import com.atguigu.bean.{ApacheLogEvent, PageViewCount}
import com.atguigu.function.{MyPageCountAgg, MyPageCountWindowResult, MyTopNHotPage}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * 实时流量统计 -> 热门页面浏览量TopN
 */
object NetWorkFlow {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic( TimeCharacteristic.EventTime )

    val dataDStream: DataStream[String] = env
      .readTextFile("D:\\MyWork\\WorkSpaceIDEA\\UserBehaviorAnalysis\\NetworkFlowAnalysis\\src\\main\\resources\\apache.log")
      .map(data => {
        val dataArray: Array[String] = data.split(" ")
        val simpleDataFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val timestamp = simpleDataFormat.parse(dataArray(3)).getTime
        ApacheLogEvent( dataArray(0), dataArray(1), timestamp, dataArray(5), dataArray(6) )
      })
      .assignTimestampsAndWatermarks( new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent]
      ( Time.minutes(1) ) {
        override def extractTimestamp(element: ApacheLogEvent): Long = element.eventTime
      } )
      .filter( _.method == "GET" )
      .filter( data => {
        val pattern = "^((?!\\.(css|js)$).)*$".r
        (pattern findFirstIn data.url).nonEmpty
      } )
      .keyBy( _.url ) // 按照页面URL做分组，开窗聚合统计
      .timeWindow( Time.minutes(10), Time.seconds(5) )
      .allowedLateness(Time.minutes(1))
      .sideOutputLateData(new OutputTag[ApacheLogEvent]("late"))
      .aggregate( MyPageCountAgg(), MyPageCountWindowResult() )
      .keyBy( _.windowEnd )
      .process( MyTopNHotPage(3) )


    dataDStream.print()
    dataDStream.getSideOutput(new OutputTag[ApacheLogEvent]("late")).print("late")

    env.execute( "hot page test" )
  }
}
