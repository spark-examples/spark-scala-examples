package com.sparkbyexamples.spark.rdd

import org.apache.spark.sql.SparkSession

object RDDPrint extends App{

  val spark:SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExample")
    .getOrCreate()
  val dept = List(("Finance",10),("Marketing",20),
      ("Sales",30), ("IT",40))
  val rdd=spark.sparkContext.parallelize(dept)
  println(rdd)
  val dataColl=rdd.collect()
  println(dataColl)
  dataColl.foreach(println)

  dataColl.foreach(f=>println(f._1 +","+f._2))
  val dataCollLis=rdd.collectAsMap()
  dataCollLis.foreach(f=>println(f._1 +","+f._2))

}
