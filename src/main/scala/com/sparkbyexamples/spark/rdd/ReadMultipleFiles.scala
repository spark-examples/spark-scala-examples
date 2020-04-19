package com.sparkbyexamples.spark.rdd

object ReadMultipleFiles extends App {

  import org.apache.spark.sql.SparkSession

  object ReadMultipleFiles extends App {

    val spark:SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    println("read all text files from a directory to single RDD")
    val rdd = spark.sparkContext.textFile("C:/tmp/files/*")
    rdd.foreach(f=>{
      println(f)
    })

    println("read text files base on wildcard character")
    val rdd2 = spark.sparkContext.textFile("C:/tmp/files/text*.txt")
    rdd2.foreach(f=>{
      println(f)
    })

    println("read multiple text files into a RDD")
    val rdd3 = spark.sparkContext.textFile("C:/tmp/files/text01.txt,C:/tmp/files/text02.txt")
    rdd3.foreach(f=>{
      println(f)
    })

    println("Read files and directory together")
    val rdd4 = spark.sparkContext.textFile("C:/tmp/files/text01.txt,C:/tmp/files/text02.txt,C:/tmp/files/*")
    rdd4.foreach(f=>{
      println(f)
    })


    val rddWhole = spark.sparkContext.wholeTextFiles("C:/tmp/files/*")
    rddWhole.foreach(f=>{
      println(f._1+"=>"+f._2)
    })

    val rdd5 = spark.sparkContext.textFile("C:/tmp/files/*")
    val rdd6 = rdd5.map(f=>{
      f.split(",")
    })

    rdd6.foreach(f => {
      println("Col1:"+f(0)+",Col2:"+f(1))
    })

  }
}
