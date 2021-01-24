package com.sparkbyexamples.spark.rdd.functions

import org.apache.spark.sql.SparkSession

object SortByKeyExample extends App{

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  val data = Seq(("Project","A", 1),
    ("Gutenberg’s", "X",3),
    ("Alice’s", "C",5),
    ("Adventures","B", 1)
  )

  val rdd=spark.sparkContext.parallelize(data)
  rdd.foreach(println)
  val rdd2=rdd.map(f=>{(f._2, (f._1,f._2,f._3))})
  rdd2.foreach(println)
  val rdd3= rdd2.sortByKey()
  val rdd4= rdd2.sortByKey(false)
  rdd4.foreach(println)

  val rdd5 = rdd.sortBy(f=>(f._3,f._2),false)
  rdd5.foreach(println)
}
