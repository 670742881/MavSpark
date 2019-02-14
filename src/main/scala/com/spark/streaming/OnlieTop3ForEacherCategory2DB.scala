//package com.spark.streaming
//
//
//
//import kafka.serializer.StringDecoder
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.{Row, SparkSession}
//import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
//import org.apache.spark.streaming.dstream.DStream
//import org.apache.spark.streaming.kafka.KafkaUtils
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.apache.spark.{Partitioner, SparkConf, SparkContext}
//
//object OnlieTop3ForEacherCategory2DB {
//  def main(args: Array[String]): Unit = {
//
//    val spark = SparkSession.builder()
//      .appName("OnLineTheTop3ItemForEachCategory2DB")
//      .master("local[2]")
//      .config("spark.sql.shuffle.partitions","5")
//      .getOrCreate()
//
//    val sc = spark.sparkContext
//    val ssc = new StreamingContext(sc, Seconds(5))
//
//
//    val kafkaParams = Map("metadata.broker.list" -> "bigdata.server1:9092",
//      "auto.offset.reset" -> "smallest",
//      "group.id" -> "KafkaDirect01")
//    val topics = Set("clickstat")
//    //1e3a167b7d3b41f6b2bc831087da4171,HUAWEI,Mate10
//    val line: DStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics).map(_._2)
//    //得到kafkaDStreaming
//    //得到DStream
//    val formattedUserClickDstream: DStream[(String, Int)] = line.filter(_.nonEmpty)
//      .flatMap(_.split(","))
//      .filter(_.length < 3)
//      .map(arr =>(( arr(1) + "_" + arr(2)), 1))
//    formattedUserClickDstream.print(1)
//
//    Partitioner
//    val ShopClickDtream=formattedUserClickDstream
//      .reduceByKeyAndWindow(
//        ((a:Int,b:Int)=>a+b),
//        Seconds(15),
//        Seconds(10),
//        4
//      )
//   //((HUAWEI_Mate10),1)
//    ShopClickDtream.foreachRDD(foreachFunc = rdd => {
//      if (rdd.isEmpty()) {
//        println("No data inputted")
//      } else {
//        //(HuaWei_p20,5905)(Oppo_Findx,3672)
//        val categoryitemRow: RDD[Row] = rdd.map(reduceItem => {
//          val category = reduceItem._1.split("_")(0)
//          val item = reduceItem._1.split("_")(1)
//          val click_count = reduceItem._2
//          Row(category, item, click_count)
//
//          val structType = StructType(Seq(StructField("category", StringType, true),
//            StructField("item", StringType, true),
//            StructField("count", IntegerType, true)
//          ))
//          val shopClickDataFrame = spark.createDataFrame(categoryitemRow, structType)
//
//          shopClickDataFrame.createTempView("categoryItemTable")
//          spark.sql("select * from categoryItemTable").show(3)
//
//          val res = spark.sql(
//            """select
//              |category,item,rank
//              |(select
//              |category,item,count ,row_number()
//              |over (partition by category order by count)rank
//              |form categoryItemTable)
//              |where rank>3
//              |
//
//              |
//            """.stripMargin)
//          res.write.json("data/log")
//
//
//
