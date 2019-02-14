package com.spark.com.sparkStreaming

import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object KafkaReceiverWC2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("sparkStreaming 和 kafka 集成方式：基于接收器的2")
      .setMaster("local[2]")

    val sc = SparkContext.getOrCreate(conf)

    val ssc = new StreamingContext(sc,Seconds(5));


    //第1步，定义数据源创建InputStreaminng
    /**
      * sparksteaming支持的数据源有两个类别：
      *   --1.基础数据源 由streamingcontext提供的api  socketTextStream
      *   --2.高级数据源 由外部工具类提供 kafak
       */

    //指定当前消费的kafaka的topic以及对应的分区数
    val topics = Map("test1" -> 4)

    val lines = KafkaUtils.createStream(
        ssc,
      "KafkaReceiverWC02",
      "bigdata.server1:2181",
        topics
    ).map(_._2)

    //第2步 调用DStream的转换操作实现计算需求
    val wordcount = lines
      .flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_,1))
      .reduceByKey(_+_)

    //第3步 调用DStrem的输出操作进行打印或者保存
    wordcount.print()

    //第4步 接收并处理数据
    ssc.start()

    //第5步 等待被终止
    ssc.awaitTermination()

  }
}
