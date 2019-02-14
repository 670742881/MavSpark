package com.spark.sql

import org.apache.spark.sql.SparkSession

object SparkSqlDemo {
  def main(args: Array[String]): Unit = {
         val spark=SparkSession
      .builder()
      .appName("the instance of session")
        .master("local[2]")
      .getOrCreate()
    val path="hdfs://bigdata.server1:8020/spark/people.json"
    val df=spark.read.json(path)
      df.show()
   df.createTempView("emp")
    import  spark.sql
/*
    sql("select * from emp").show
    sql("select name,age+1 from emp").show()
    sql("select name from emp where age>20").show()
    sql("select count(*) from emp where age>20").show()
**/
  // sdl语句
    df.select("name").show()
    df.select(df("name"),df("age")+1).show()
    df.filter(df("age")>20).show()
    df.groupBy(df("age")>20).count().show()

    //保存dataframe 结果
    //df.write.parquet("")
    df.write.save("spark/output")
  }

}
