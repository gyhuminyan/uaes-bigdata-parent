package com.uaes.spark.analysis.Laucher

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by mzhang on 2017/10/16.
  */
object TestSpark {
  def main(args: Array[String]): Unit = {
    testJoin()
  }

  def testJoin(): Unit ={
    val conf = new SparkConf()
    conf.setAppName("test rdd ").setMaster("local[*]")

    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(Array((2,"b"),(3,"c"),(2,"d"),(2,"f")))
    val rdd2 = sc.parallelize(Array((2,1),(2,2),(2,5)))
    val rdd = rdd2.rightOuterJoin(rdd1)
    rdd.collect().foreach(println)

    sc.stop()
  }
}
