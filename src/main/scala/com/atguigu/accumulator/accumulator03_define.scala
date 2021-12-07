package com.atguigu.accumulator

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object accumulator03_define {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(List("Hello", "Halo", "Hi", "Hello", "Spark", "Spark"), 2)
    val acc: MyAcc = new MyAcc
    sc.register(acc)
    rdd.foreach(word => acc.add(word))
    println(acc.value)


    //4.关闭连接
    sc.stop()
  }

  class MyAcc extends AccumulatorV2[String,mutable.Map[String,Int]]{
    var map:mutable.Map[String, Int]  = mutable.Map()

    override def isZero: Boolean = map.isEmpty

    override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = new MyAcc

    override def reset(): Unit = map.clear()

    override def add(v: String): Unit = {
      if (v.startsWith("H")){
        map(v) = map.getOrElse(v,0) + 1
      }
    }

    override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
      val otherMap: mutable.Map[String, Int] = other.value
     otherMap.foreach({
       case (key,value) => map(key) = map.getOrElse(key,0) + 1
     })
    }

    override def value: mutable.Map[String, Int] = map
  }
}
