package com.spark.sql

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSqlJson {
  def main(args: Array[String]): Unit = {

    //创建SparkSession
    val spark=SparkSession.builder()
      .appName(getClass.getSimpleName)
      .master("local[2]")
      .getOrCreate()

    //读取parquet文件形成dataframe
   val df = spark.read.parquet("/spark/data/users.parquet")
    df.show

    //dsl语句查询
    import spark.implicits._
    import org.apache.spark.sql.functions._
    println("//dsl语句查询================")
    df.select("name").show
    df.select($"name",$"favorite_color").show()
    df.select(df("name"),df("favorite_numbers")(0).alias("第一个爱好的数字")).show
    df.select(col("name"),col("favorite_color"),df("favorite_numbers")(2)).show()
    df.filter(df("favorite_color").notEqual(" ")).show()
    df.groupBy("name").count.show()

    println("//sql语句查询================")
    df.createTempView("user")
    spark.sql(
      """
        |select *
        |from  user
        |
        |
      """.stripMargin).show
    spark.sql(
      """
        |select
        |name,favorite_numbers,favorite_color
        |from
        |user
        |where favorite_color <> " "
      """.stripMargin).show



  }

}
