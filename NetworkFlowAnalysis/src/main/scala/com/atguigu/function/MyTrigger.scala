package com.atguigu.function

import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
 * 触发器：
 */
case class MyTrigger() extends Trigger[(String, Long), TimeWindow]{
  // TriggerResult.FIRE_AND_PURGE 触发计算 关闭窗口清空所有状态
  override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE_AND_PURGE

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}
}
