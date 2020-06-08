package com.atguigu.orderpay_detect

import com.atguigu.bean.{OrderEvent, ReceiptEvent}
import com.atguigu.function.TxPayMatchDetectByJoin
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object TxMatchWithJoin {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 1. 读取数据转换成样例类
    val orderResource = getClass.getResource("/OrderLog.csv")
    val orderEventStream: KeyedStream[OrderEvent, String] = env.readTextFile(orderResource.getPath)
      //    val orderEventStream = env.socketTextStream("hadoop102", 7777)
      .map(data => {
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .filter( _.txId != "" )    // 只要pay事件
      .keyBy(_.txId)


    val receiptResource = getClass.getResource("/ReceiptLog.csv")
    val receiptEventStream: KeyedStream[ReceiptEvent, String] = env.readTextFile(receiptResource.getPath)
      //    val receiptEventStream = env.socketTextStream("localhost", 8888)
      .map(data => {
        val dataArray = data.split(",")
        ReceiptEvent(dataArray(0), dataArray(1), dataArray(2).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .keyBy(_.txId)

    // 2. 连接两条流，做分别计算
    val resultStream: DataStream[(OrderEvent, ReceiptEvent)] = orderEventStream
      .intervalJoin( receiptEventStream )
      .between( Time.seconds(-3), Time.seconds(5) )
      .process( TxPayMatchDetectByJoin() )

    // 3. 打印输出
    resultStream.print("matched")

    env.execute("tx match with join job")
  }
}
