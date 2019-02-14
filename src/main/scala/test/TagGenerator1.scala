package test

import com.javabean.ReviewTags
import org.apache.spark.{SparkConf, SparkContext}


object TagGenerator1 {
  def main(args: Array[String])= {
    val sc = new SparkContext(new SparkConf().setAppName("TagGenerator by ***").setMaster("local[2]"))
    //  val sqlContext = new HiveContext(sc)
    // import sqlContext.implicits._

    val poi_tags = sc.textFile("data/temptags.txt") // to be replaced by hive data resources
    val poi_taglist = poi_tags.map(e=>e.split("\t")).filter(e=>e.length == 2).
      map(e=>e(0)->ReviewTags.extractTags(e(1))).
      filter(e=> e._2.length > 0).map(e=> e._1 -> e._2.split(",")).
      flatMapValues(e=>e).
      map(e=> (e._1,e._2)->1).
      reduceByKey(_+_).
      map(e=> e._1._1 -> List((e._1._2,e._2))).
      reduceByKey(_:::_).
      map(e=> e._1-> e._2.sortBy(_._2).reverse.take(100).map(a=> a._1+":"+a._2.toString).mkString(","))
    poi_taglist.map(e=>e._1+"\t"+e._2).saveAsTextFile("data/----path to dest data on hdfs----")
  }
}
