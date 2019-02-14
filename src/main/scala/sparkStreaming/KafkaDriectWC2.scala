package com.spark.com.sparkStreaming

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object KafkaDriectWC2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Spark Steaming WordCount Demo")
      .setMaster("local[2]")

    val sc = SparkContext.getOrCreate(conf)

    val ssc = new StreamingContext(sc,Seconds(5))
    //第1步，定义数据源创建InputStreaminng

    //给定consumer的配置参数
    val kafkaParams = Map(
      "metadata.broker.list"->"bigdata.server1:9092,bigdata.server1:9093,bigdata.server1:9094,bigdata.server1:9095",
      "auto.offset.reset"-> "smallest",
      "group.id" ->"KafkaDirect01"
    )
    //给定消费的topic
    val topics = Set("test")


    val lines = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](
      ssc,
      kafkaParams,
      topics
    ).map(_._2)

    //第2步，调用DStream的transform操作，完成需求
    val wordcount = lines.flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_,1))
      .reduceByKey(_+_)

    //第3步，调用输出操作，对结果进行打印或者保存
    wordcount.print()

    //  wordcount.saveAsTextFiles("hdfs://bigdata.server1:8020/spark/streamOut-",suffix = s"${System.currentTimeMillis()}")

    //第4步，启动接收数
    ssc.start()


    //第5步，等待被终止
    ssc.awaitTermination()

  }
}
