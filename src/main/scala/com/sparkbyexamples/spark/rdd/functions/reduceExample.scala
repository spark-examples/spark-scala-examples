package com.sparkbyexamples.spark.rdd.functions

import org.apache.spark.sql.SparkSession

object reduceExample extends App {

  val spark = SparkSession.builder()
    .appName("SparkByExamples.com")
    .master("local[3]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val listRdd = spark.sparkContext.parallelize(List(1,2,3,4,5,3,2))

  println("output min using binary : "+listRdd.reduce(_ min _))
  println("output max using binary : "+listRdd.reduce(_ max _))
  println("output sum using binary : "+listRdd.reduce(_ + _))


  // Alternatively you can write
  println("output min : "+listRdd.reduce( (a,b) => a min b))
  println("output max : "+listRdd.reduce( (a,b) => a max b))
  println("output sum : "+listRdd.reduce( (a,b) => a + b))


  val inputRDD = spark.sparkContext.parallelize(List(("Z", 1),("A", 20),("B", 30),
    ("C", 40),("B", 30),("B", 60)))

  println("output min : "+inputRDD.reduce( (a,b)=> ("max",a._2 min b._2))._2)
  println("output max : "+inputRDD.reduce( (a,b)=> ("max",a._2 max b._2))._2)
  println("output sum : "+inputRDD.reduce( (a,b)=> ("Sum",a._2 + b._2))._2)
}
