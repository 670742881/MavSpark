package com.spark.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

object SQLDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("the instance of Spark Seession")
      .master("local[2]")
      .getOrCreate()

    val file = "/spark/resources/employees.json"
  //第一步，使用sarpkSessio对象读取外部数据源形成Dataframe
  val df: DataFrame = spark.read.json(file)

    //查看datafrmae的内容
    df.show()
    //第二步，可以使用sql或者是DSL(域定义语言)完成业务需求
    //第一种方式 使用sql
    df.createTempView("emp")
    //1.查询“name”列
    import spark.sql
    sql("select name from emp").show

    //2.查询“name”列，和“scalary”列，但是salary要加上500
    sql("select name,salary+500 from emp").show()

    //3.查询“sclary”> 4000
   sql("select * from emp where salary >= 4000").show()

    //4.按salary分组统计
    sql("select count(*) from emp group by salary >= 4000").show

    //第2种使用DSL语句
    df.select("name").show()
    df.select(df("name"),df("salary")+500).show
    df.filter(df("salary") >= 4000).show
    df.groupBy(df("salary") >= 4000).count().show

    //第三步，保存或者展示dataframe的数据
    df.write.save("/spark/sqlout")
  }
}
