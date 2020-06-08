package com.atguigu.orderpay_detect

import com.atguigu.bean.{OrderEvent, OrderResult}
import com.atguigu.function.OrderPayMatcchDetect
import com.atguigu.orderpay_detect.OrderTimeout.getClass
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._

object OrderTimeoutWithoutCep {
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

    // 按照orderId分组处理 主流正常支付结果，测输出流订单超时
    val orderResultStream: DataStream[OrderResult] = orderEventStream
      .keyBy(_.orderId)
      .process( OrderPayMatcchDetect() )

    orderResultStream.print( "payed" )
    orderResultStream.getSideOutput( new OutputTag[OrderResult]("timeout") ).print("timeout")

    env.execute( "order timeout without cep job" )


  }
}
