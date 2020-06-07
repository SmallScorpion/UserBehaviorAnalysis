package com.atguigu.loginfail_detect

import com.atguigu.bean.{LoginEvent, LoginFailWarning}
import com.atguigu.function.LoginFailDetect
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object LoginFailWithCep {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 1. 读取数据，map成样例类
        val resource = getClass.getResource("/LoginLog.csv")
    //    val loginEventStream: DataStream[LoginEvent] = env.readTextFile(resource.getPath)

    val loginEventStream: DataStream[LoginEvent] = env.readTextFile(resource.getPath)
      .map( data => {
        val dataArray = data.split(",")
        LoginEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
      } )
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(0)) {
        override def extractTimestamp(element: LoginEvent): Long = element.timestamp * 1000L
      })

    // 2. 构造一个模式pattern
    val loginFailPattern: Pattern[LoginEvent, LoginEvent] = Pattern
      .begin[LoginEvent]("firstFail").where(_.eventType == "fail")    // 第一次登录失败事件
      .next("secondFail").where(_.eventType == "fail")    // 紧跟着第二次登录失败事件
      .within(Time.seconds(2))    // 在2秒之内连续发生有效

    // 循环模式定义实例
    val singleFailPattern: Pattern[LoginEvent, LoginEvent] = Pattern
      // 10秒内两次fail
      .begin[LoginEvent]("fail").times(2).where(_.eventType == "fail")
      .within( Time.seconds(10) )


    // 3. 将pattern应用到dataStream上，得到一个PatternStream
    val patternStream: PatternStream[LoginEvent] = CEP.pattern(loginEventStream.keyBy(_.userId), loginFailPattern)
      .sideOutputLateData(new OutputTag[LoginEvent]("late"))

    // 4. 检出符合规则匹配的复杂事件，转换成输出结果
    val loginFailWarningStream: DataStream[LoginFailWarning] = patternStream
      .select( new LoginFailDetect() )

    // 5. 打印输出报警信息
    loginFailWarningStream.print("warning")

    env.execute("login fail with cep job")
  }
}
