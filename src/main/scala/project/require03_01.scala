package project

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object require03_01 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    // 读取数据源
    val lineRDD: RDD[String] = sc.textFile("input/user_visit_action.txt")

    val userVisitActionRDD: RDD[UserVisitAction] = lineRDD.map(action => {
      val data: Array[String] = action.split("_")
      UserVisitAction(
        data(0),
        data(1),
        data(2),
        data(3),
        data(4),
        data(5),
        data(6),
        data(7),
        data(8),
        data(9),
        data(10),
        data(11),
        data(12)
      )
    })

    userVisitActionRDD.cache()

    val list: List[String] = List("1", "2", "3", "4", "5", "6", "7")
    val denominator: Broadcast[List[String]] = sc.broadcast(list)

    val list1: List[(String, String)] = list.zip(list.tail)
    val molecule: Broadcast[List[(String, String)]] = sc.broadcast(list1)

    val pageCountDenoRDD: RDD[(String, Int)] = userVisitActionRDD
      .filter(data => denominator.value.contains(data.page_id))
      .map(user => (user.page_id, 1))
      .reduceByKey(_ + _)

    // 计算出的分母的数量
    val denoArray: Array[(String, Int)] = pageCountDenoRDD.collect()


    // 计算分子
    val pageJumpRDD: RDD[(String, List[(String, String)])] = userVisitActionRDD
      .map(action => (action.session_id, (action.action_time, action.page_id)))
      .groupByKey()
      .mapValues(ite => {
        val pageJump: List[String] = ite.toList.sortBy(_._1).map(_._2)
        pageJump.zip(pageJump.tail)
      })

//    pageJumpRDD.collect().foreach(println)

    val pageToPageOneRDD: RDD[(String, String)] = pageJumpRDD
      .flatMap(_._2)
      .filter(tuple => molecule.value.contains(tuple))

//    pageToPageOneRDD.collect().foreach(println)
    val moleculeRDD: RDD[((String, String), Int)] = pageToPageOneRDD
      .map(tuple => (tuple, 1))
      .reduceByKey(_ + _)

    val tuples: Array[((String, String), Int)] = moleculeRDD.collect()
//    println(tuples.mkString(","))

    val denoMap: Map[String, Int] = denoArray.toMap
    val result: mutable.Map[(String, String),Double] = mutable.Map()

    for (elem <- tuples) {
      result(elem._1) = elem._2.toDouble / denoMap(elem._1._1)
    }

    println(result)

    //4.关闭连接
    sc.stop()
  }
}
