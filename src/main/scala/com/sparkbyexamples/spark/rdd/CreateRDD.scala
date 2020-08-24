package com.sparkbyexamples.spark.rdd

import org.apache.spark.sql.SparkSession

object CreateRDD {

  def main(args:Array[String]): Unit ={

    val spark:SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("SparkByExamples.com")
      .getOrCreate()

    val rdd=spark.sparkContext.parallelize(Seq(("Java", 20000), ("Python", 100000), ("Scala", 3000)))
    rdd.foreach(println)

    val rdd1 = spark.sparkContext.textFile("/path/textFile.txt")

    val rdd2 = spark.sparkContext.wholeTextFiles("/path/textFile.txt")
    rdd2.foreach(record=>println("FileName : "+record._1+", FileContents :"+record._2))

    val rdd3 = rdd.map(row=>{(row._1,row._2+100)})
    rdd3.foreach(println)

    val myRdd2 = spark.range(20).toDF().rdd
    myRdd2.foreach(println)



  }
}
