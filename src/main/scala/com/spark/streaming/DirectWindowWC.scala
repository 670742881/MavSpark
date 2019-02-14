package com.spark.streaming


import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DirectWindowWC {
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf()
      .setMaster("local[2]")
      .setAppName(getClass.getSimpleName)
   val sc=SparkContext.getOrCreate(conf)
    val ssc= new StreamingContext(sc,Seconds(5))

   //
    val topics=Array("test")
      val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "bigdata.server1:9092,bigdata.server1:9093,bigdata.server1:9094,bigdata.server1:9095",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "reduceByKeyAndWindow and updateStateByKey",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val line=KafkaUtils.createDirectStream(
      ssc,
      PreferConsistent ,
      Subscribe[String, String](topics, kafkaParams)
    ).map(_.value())
    val wc1=line.flatMap(_.split(" "))
          .filter(_.nonEmpty)
      .map(word=>(word,1))
        .reduceByKeyAndWindow(
           ((a:Int,b:Int)=>a+b),
          Seconds(15),
          Seconds(10)
        )
     ssc.checkpoint("data/checkpoint")
    val wc2=line.flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map((_,1))
      .reduceByKey(_+_)
      .updateStateByKey((seq:Seq[Int],state:Option[Long])=>{
        val currentValue=seq.sum
        val previousValue=state.getOrElse(0l)
      Some(currentValue+previousValue)
      //  Option.apply(currentValue+previousValue)
    }
      )

   wc2.print()
   // wc1.print()

    ssc.start()
    ssc.awaitTermination()
  }



}
