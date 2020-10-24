package com.sparkbyexamples.spark.dataframe.examples

import org.apache.spark.sql.SparkSession

object ForEachExample extends App {

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  val data = Seq(("Banana",1000,"USA"), ("Carrots",1500,"USA"), ("Beans",1600,"USA"),
    ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"),
    ("Carrots",1200,"China"),("Beans",1500,"China"))

  //DataFrame
  val df = spark.createDataFrame(data).toDF("Product","Amount","Country")
  df.foreach(f=> println(f))

  val longAcc = spark.sparkContext.longAccumulator("SumAccumulator")
  df.foreach(f=> {
    longAcc.add(f.getInt(1))
  })
  println("Accumulator value:"+longAcc.value)
  //rdd
  val rdd = spark.sparkContext.parallelize(Seq(1,2,3,4,5,6,7,8,9))
  rdd.foreach(print)

  //rdd accumulator
  val rdd2 = spark.sparkContext.parallelize(Seq(1,2,3,4,5,6,7,8,9))
  val longAcc2 = spark.sparkContext.longAccumulator("SumAccumulator2")
  rdd .foreach(f=> {
    longAcc2.add(f)
  })
  println("Accumulator value:"+longAcc2.value)
}