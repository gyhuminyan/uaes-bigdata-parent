package com.uaes.spark.common.Laucher

/**
  * Created by mzhang on 2017/10/16.
  */
object TestSpark {
  def main(args: Array[String]): Unit = {
    println("123")
    val map1 = Map("123"->1,"234"->2)
    val c = map1.map(pair => (pair._1,pair._2 * 2))
    println(c)

  }
}
