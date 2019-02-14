package com.spark.com.sparkStreaming

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object KafkaReceiverWC3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("sparkStreaming 和 kafka 集成方式：基于接收器的2")
      .setMaster("local[4]")   //多个接收器的情况下，每个接收器需要1个线程，还需要至少1个线程来计算数据
      .set("spark.streaming.blockInterval","500ms")  //根据实际的需求调整blockInterval的时间，如果为了增加实时性，调小该参数，反之，增大该参数
      .set("spark.streaming.receiver.writeAheadLog.enable","true") //开启wal日志，实现数据的零丢失

    val sc = SparkContext.getOrCreate(conf)

    val ssc = new StreamingContext(sc,Seconds(5));
    ssc.checkpoint("checkpoint")

    //第1步，定义数据源创建InputStreaminng
    /**
      * sparksteaming支持的数据源有两个类别：
      *   --1.基础数据源 由streamingcontext提供的api  socketTextStream
      *   --2.高级数据源 由外部工具类提供 kafak
       */

    //指定当前消费的kafaka的topic以及对应的分区数
    val topics = Map("test1" -> 4)

    //采用多个接收器接收数据，好处可以实现接收数据的负载均衡和容错
    val lines1 = KafkaUtils.createStream(
        ssc,
      "bigdata.server1:2181",
      "KafkaReceiverWC03",
        topics
    )

    val lines2 = KafkaUtils.createStream(
      ssc,
      "bigdata.server1:2181",
      "KafkaReceiverWC02",
      topics
    )

    val lines3 = KafkaUtils.createStream(
      ssc,
      "bigdata.server1:2181",
      "KafkaReceiverWC02",
      topics
    )

    //必须要将多个接收器接收的数据进行合并union
    val lines: DStream[(String, String)] = lines1.union(lines2).union(lines3)

    //第2步 调用DStream的转换操作实现计算需求
    val wordcount = lines.map(_._2)
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
