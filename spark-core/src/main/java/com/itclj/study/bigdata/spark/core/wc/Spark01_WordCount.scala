package com.itclj.study.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_WordCount {
  def main(args: Array[String]): Unit = {
    //Application
    //Spark框架
    //TODO 建立与spark 框架的连接
    val sparkConf = new SparkConf().setMaster("local").setAppName("ItcljWorldCount");
    val sc = new SparkContext(sparkConf);
    //TODO 执行业务操作
    //1. 读取文件，读取一行一行的数据
    // hello word
    val lines: RDD[String] = sc.textFile(path = "datas")
//    val arrayLines = lines.collect()
//    arrayLines.foreach(println)

    //2. 将一行一行的数据，拆成单词
    //扁平化：将整体拆分成个体的操作
    //“hello word” =》hello word,hello word
    val words: RDD[String] = lines.flatMap(_.split( " "))

    //3. 将数据根据单词分组，便于统计
    //(hello,hello,hello),(word,word)
    val wordGroup = words.groupBy(word => word)

    //4. 分组后进行转换
    //(hello,hello,hello),(word,word)
    //(hello,3),(word,2)
    val wordToCount = wordGroup.map {
      case (word, list) => {
        (word, list.size)
      }
    }

    //5.将转换结构，采集到控制台打印
    val array = wordToCount.collect()
    array.foreach(println)

    //TODO 关闭连接
    sc.stop();
  }
}
