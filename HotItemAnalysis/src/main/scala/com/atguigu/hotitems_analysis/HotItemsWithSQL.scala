package com.atguigu.hotitems_analysis

import com.atguigu.bean.UserBehavior
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

/**
 * 热门商品统计 SQL实现
 */
object HotItemsWithSQL {
  def main(args: Array[String]): Unit = {
    // 创建一个流处理执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 创建表执行环境
    val settings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv = StreamTableEnvironment.create(env, settings)

    // 从文件读取数据
    val inputStream: DataStream[String] = env.readTextFile("D:\\Projects\\BigData\\UserBehaviorAnalysis\\HotItemAnalysis\\src\\main\\resources\\UserBehavior.csv")
    // 转换成样例类类型，并分配时间戳和watermark
    val dataStream: DataStream[UserBehavior] = inputStream
      .map( data => {
        val dataArray = data.split(",")
        UserBehavior( dataArray(0).toLong, dataArray(1).toLong, dataArray(2).toInt, dataArray(3), dataArray(4).toLong )
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 把流直接注册成表，并提取需要的字段，定义时间属性
    tableEnv.createTemporaryView("dataTable", dataStream, 'itemId, 'behavior, 'timestamp.rowtime as 'ts)

    val resultTable = tableEnv.sqlQuery(
      """
        |select *
        |from (
        |  select *, row_number() over (partition by windowEnd order by cnt desc) as row_num
        |  from (
        |    select itemId, hop_end(ts, interval '5' minute, interval '1' hour) as windowEnd, count(itemId) as cnt
        |    from dataTable
        |    where behavior = 'pv'
        |    group by itemId, hop(ts, interval '5' minute, interval '1' hour)
        |    )
        |  )
        |where row_num <= 5
      """.stripMargin)

    resultTable.toRetractStream[Row].print("result")

    env.execute("hot items with sql job")
  }
}
