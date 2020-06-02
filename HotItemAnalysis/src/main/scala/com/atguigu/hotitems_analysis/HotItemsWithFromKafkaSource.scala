package com.atguigu.hotitems_analysis

import java.util.Properties

import com.atguigu.bean.{ItemViewCount, UserBehavior}
import com.atguigu.function.{MyCountAggFunction, MyTopNHotItems, MyWindowResultFunction}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
 * 将 文件中的数据 经过读取发送到Kafka，再消费Kafka的数据
 */
object HotItemsWithFromKafkaSource {
  def main(args: Array[String]): Unit = {

    // 创建一个流处理执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic( TimeCharacteristic.EventTime ) // 开启事件事件
    env.setParallelism(1) // 设置并行度为1

    // 配置Kafka消费者， 读取hotitems的数据输出到控制台
    val properties: Properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop102:9092")
    properties.setProperty("group.id", "consumer-group")
    // 从kafka读取数据
    val inputDStream: DataStream[String] = env
      .addSource( new FlinkKafkaConsumer[String]("hotitems", new SimpleStringSchema(), properties) )

    // 转换成样例类
    val dataDStream: DataStream[UserBehavior] = inputDStream
      .map(
        data => {
          val dataArray: Array[String] = data.split(",")
          UserBehavior( dataArray(0).toLong, dataArray(1).toLong, dataArray(2).toInt, dataArray(3), dataArray(4).toLong)
        }
      )
      .assignAscendingTimestamps(_.timestamp * 1000L) // 由于数据时间字段是升序可直接使用此方法

    // 进行开窗聚合转换
    val aggDStream: DataStream[ItemViewCount] = dataDStream
      .filter( _.behavior == "pv" ) // 过滤pv字段
      .keyBy("itemId") // 以itemId为key进行聚合
      .timeWindow( Time.hours(1), Time.minutes(5) )  // 开滑动窗口 窗口大小为1小时，滑动步长为 5分钟
      .aggregate( MyCountAggFunction(), MyWindowResultFunction() )

    // 对统计聚合结果按照窗口分组，排序输出
    val resultDStream: DataStream[String] = aggDStream
      .keyBy("windowEnd")
      .process( MyTopNHotItems(5) )

    // 测试打印输出
    resultDStream.print()

    env.execute("hot items job")
  }
}
