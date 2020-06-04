package com.atguigu.function

/**
 * 自定义布隆过滤器
 * @param size
 */
case class MyBloom(size: Long){
  // 一般取cap是2的整次方
  private val cap = size
  // 实现一个hash函数
  def hash( value: String, seed: Int ): Long ={
    var result = 0L
    for( i <- 0 until value.length ){
      // 用每个字符的ascii码值做叠加计算
      result = result * seed + value.charAt(i)
    }
    // 返回一个cap范围内hash值
    (cap - 1) & result
  }
}
