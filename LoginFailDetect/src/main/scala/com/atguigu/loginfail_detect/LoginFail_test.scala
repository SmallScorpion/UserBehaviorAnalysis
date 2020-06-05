package com.atguigu.loginfail_detect

import com.atguigu.bean.{LoginEvent, LoginFailWarning}
import com.atguigu.function.{LoginFailDetectWarning, LoginFailDetectWarning_test}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


object LoginFail_test {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource = getClass.getResource("/LoginLog.csv")
    val loginEventStream: DataStream[LoginFailWarning] = env.readTextFile(resource.getPath)
      .map( data => {
        val dataArray = data.split(",")
        LoginEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
      } )
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(3)) {
        override def extractTimestamp(element: LoginEvent): Long = element.timestamp * 1000L
      })

      // 自定义ProcessFunction，通过注册定时器实现判断2s内连续登录失败的需求
      .keyBy(_.userId)     // 按照用户id分组检测
      .process( LoginFailDetectWarning_test(2) )

    loginEventStream.print()

    env.execute("login fail job")
  }
}


