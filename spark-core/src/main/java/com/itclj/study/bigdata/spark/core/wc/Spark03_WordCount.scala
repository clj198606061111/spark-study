package com.itclj.study.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * var 和 val区别
 *
 * var是一个可变变量
 * val是一个只读变量
 */
object Spark03_WordCount {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("ItcljWorldCount");
    val sc = new SparkContext(sparkConf);
    val lines: RDD[String] = sc.textFile(path = "datas")
    val words: RDD[String] = lines.flatMap(_.split(" "))
    //转为map，方便统计
    val worldToOne = words.map(world => (world, 1))

    //spark提供了更多的功能，分组和聚合用一个方法实现
    //reduceByKey:相同的key的数据，可以对value进行reduce聚合
    val wordToCount = worldToOne.reduceByKey((x,y)=>{x+y})
    //worldToOne.reduceByKey(_+_)//或者这样写，自解原则

    //5.将转换结构，采集到控制台打印
    val array = wordToCount.collect()
    array.foreach(println)

    //TODO 关闭连接
    sc.stop();
  }
}
