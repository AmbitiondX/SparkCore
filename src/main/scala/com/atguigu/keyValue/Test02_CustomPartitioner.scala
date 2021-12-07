package com.atguigu.keyValue

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object Test02_CustomPartitioner {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)
    val intRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7), 2)
    intRDD
      .map((_,1))
      .partitionBy(new myPartitioner(3))
      .map(tuple => tuple._1)
      .mapPartitionsWithIndex((num,list) => list.map(i => (num,i)))
      .collect().foreach(println)


    //4.关闭连接
    sc.stop()
  }

  class myPartitioner(num: Int) extends Partitioner{
    override def numPartitions: Int = num

    override def getPartition(key: Any): Int = {
      key match {
        case i: Int => if(i < 3) 0 else 1
        case _ => 2
      }
    }
  }
}
