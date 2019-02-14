package com.spark.kafka1_0.Streaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
object Kafka1_0_Streaming {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Spark Steaming WordCount Demo")
      .setMaster("local[2]")
    conf.set("spark.streaming.backpressure.enabled","true")//开启背压功能
    conf.set("spark.streaming.kafka.maxRatePerPartition","100")

    val sc = SparkContext.getOrCreate(conf)

    val ssc = new StreamingContext(sc,Seconds(5))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "bigdata.server1:9092,bigdata.server1:9093,bigdata.server1:9094,bigdata.server1:9095",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "kafka1.0",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("test")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

      val stream2 = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)

    )

   val line: DStream[String] = stream.map(_.value())
    val wordcount: DStream[(String, Int)] =line.flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_,1))
      .reduceByKey(_+_)
    wordcount.print()

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()


  }

}
