package com.atguigu.accumulator

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

object AvgAcc {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7))

    // 注册自定义累加器
    val acc: MyAcc = new MyAcc
    sc.register(acc)

    rdd.foreach(v => acc.add(v))
    println(acc.value._1.toDouble / acc.value._2)

    //4.关闭连接
    sc.stop()
  }
  class MyAcc extends AccumulatorV2[Int,(Int,Int)] {
    var sum: Int = 0
    var count: Int = 0

    override def isZero: Boolean = sum == 0 && count == 0

    override def copy(): AccumulatorV2[Int, (Int, Int)] = new MyAcc

    override def reset(): Unit = {
      sum = 0
      count = 0
    }

    override def add(v: Int): Unit = {
      sum += v
      count += 1
    }

    override def merge(other: AccumulatorV2[Int, (Int, Int)]): Unit = {
      sum += other.value._1
      count += other.value._2
    }

    override def value: (Int, Int) = (sum,count)
  }
}
