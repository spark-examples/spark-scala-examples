//package com.sparkbyexamples.spark.dataframe.examples
//
//import org.apache.spark.sql.SparkSession
//
//object ForEachPartExample extends App {
//
//  val spark: SparkSession = SparkSession.builder()
//    .master("local[1]")
//    .appName("SparkByExamples.com")
//    .getOrCreate()
//
//  val data = Seq(("Banana",1000,"USA"), ("Carrots",1500,"USA"), ("Beans",1600,"USA"),
//    ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"),
//    ("Carrots",1200,"China"),("Beans",1500,"China"))
//
//  // foreachPartition DataFrame
//  val df = spark.createDataFrame(data).toDF("Product","Amount","Country")
//  df.foreachPartition(partition => {
//    //Initialize any database connection
//    partition.foreach(fun=>{
//      //apply the function
//    })
//  })
//
//  //rdd
//  val rdd = spark.sparkContext.parallelize(Seq(1,2,3,4,5,6,7,8,9))
//  rdd.foreachPartition(partition => {
//    //Initialize any database connection
//    partition.foreach(fun=>{
//      //apply the function
//    })
//  })
//}