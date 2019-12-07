package com.sparkbyexamples.spark.rdd.functions

import org.apache.spark.sql.SparkSession

object foldExample extends App {

  val spark = SparkSession.builder()
    .appName("SparkByExamples.com")
    .master("local[3]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  //fold example
  val listRdd = spark.sparkContext.parallelize(List(1,2,3,4,5,3,2))
  println("Partitions : "+listRdd.getNumPartitions)
  println("Total : "+listRdd.fold(0)((acc,ele) => {acc + ele}))
  println("Total with init value 2 : "+listRdd.fold(2)((acc,ele) => {acc + ele}))
  println("Min : "+listRdd.fold(0)((acc,ele) => {acc min ele}))
  println("Max : "+listRdd.fold(0)((acc,ele) => {acc max ele}))

  val inputRDD = spark.sparkContext.parallelize(List(("Z", 1),("A", 20),("B", 30),("C", 40),("B", 30),("B", 60)))

  println("Total : "+inputRDD.fold(("",0))( (acc,ele)=>{ ("Total", acc._2 + ele._2)  }))
  println("Min : "+inputRDD.fold(("",0))( (acc,ele)=>{ ("Min", acc._2 min ele._2)  }))
  println("Max : "+inputRDD.fold(("",0))( (acc,ele)=>{ ("Max", acc._2 max ele._2)  }))

}
