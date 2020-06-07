package com.atguigu.function

import java.util

import com.atguigu.bean.{LoginEvent, LoginFailWarning}
import org.apache.flink.cep.PatternSelectFunction



// 实现自定义的PatternSelectFunction
class LoginFailDetect() extends PatternSelectFunction[LoginEvent, LoginFailWarning]{
  override def select(pattern: util.Map[String, util.List[LoginEvent]]): LoginFailWarning = {
    val firstFailEvent: LoginEvent = pattern.get("firstFail").iterator().next()
    val sencondFailEvent: LoginEvent = pattern.get("secondFail").iterator().next()
    LoginFailWarning( firstFailEvent.userId, firstFailEvent.timestamp, sencondFailEvent.timestamp, "login fail" )
  }
}
