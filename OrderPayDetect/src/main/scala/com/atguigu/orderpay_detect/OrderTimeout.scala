package com.atguigu.orderpay_detect

import com.atguigu.bean.{OrderEvent, OrderResult}
import com.atguigu.function.{OrderPaySelect, OrderTimeoutSelect}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object OrderTimeout {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic( TimeCharacteristic.EventTime )
    env.setParallelism(1)

    val resource = getClass.getResource( "/OrderLog.csv" )
    val orderEventStream: DataStream[OrderEvent] = env
      .readTextFile(resource.getPath)
      .map( data => {
        val dataArrray = data.split(",")
        OrderEvent(dataArrray(0).toLong, dataArrray(1), dataArrray(2), dataArrray(3).toLong)
      }  )
      .assignAscendingTimestamps( _.timestamp * 1000L )

    // 定义一个pattern
    val orderPayPattern = Pattern
      .begin[OrderEvent]("create").where(_.eventType == "create")
      .followedBy("pay").where( _.eventType == "pay" )
      .within( Time.minutes(15) )

    // 将pattern应用到按照orderid分组后的datastream上
    val patternStream = CEP
      .pattern( orderEventStream.keyBy(_.orderId), orderPayPattern )

    // 定义一个超时订单侧输出流标签
    val orderTimeoutOutputTag = new OutputTag[OrderResult]("timeout")

    // 调用select方法， 分别处理匹配数据和超时未匹配数据
    val resultStream: DataStream[OrderResult] = patternStream
      .select( orderTimeoutOutputTag, OrderTimeoutSelect(), OrderPaySelect() )

    // 打印输出
    resultStream.print("pay")
    resultStream.getSideOutput(orderTimeoutOutputTag).print("timeout")

    env.execute(" order timeout job test")


  }
}
