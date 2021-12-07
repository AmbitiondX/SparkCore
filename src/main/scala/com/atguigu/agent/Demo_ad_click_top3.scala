package com.atguigu.agent

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo_ad_click_top3 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    sc.textFile("input/agent.log")
      .map(line => {
        val array: Array[String] = line.split(" ")
        ((array(1),array(4)),1)
      })
      .reduceByKey(_ + _)
      .map(taple => (taple._1._1,(taple._1._2,taple._2)))
      .groupByKey()
      .mapValues({
        datas => {
          datas.toList.sortWith(_._2 > _._2)
        }.take(3)
      })
      .collect()
      .foreach(println)

//    Thread.sleep(600000)

    //4.关闭连接
    sc.stop()
  }
}
