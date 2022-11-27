package com.itclj.study.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * var 和 val区别
 *
 * var是一个可变变量
 * val是一个只读变量
 */
object Spark02_WordCount {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("ItcljWorldCount");
    val sc = new SparkContext(sparkConf);
    val lines: RDD[String] = sc.textFile(path = "datas")
    val words: RDD[String] = lines.flatMap(_.split(" "))
    val worldToOne = words.map(world => (world, 1))
    //按world分组
    val wordGroup = worldToOne.groupBy(t => t._1)
    val wordToCount = wordGroup.map {
      case (word, list) => {
        list.reduce(
          (t1, t2) => {
            (t1._1, t1._2 + t2._2)
          }
        )
      }
    }

    //5.将转换结构，采集到控制台打印
    val array = wordToCount.collect()
    array.foreach(println)

    //TODO 关闭连接
    sc.stop();
  }
}
