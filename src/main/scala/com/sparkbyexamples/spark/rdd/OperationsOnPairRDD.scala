package com.sparkbyexamples.spark.rdd

import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object OperationsOnPairRDD {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("SparkByExample")
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val rdd = spark.sparkContext.parallelize(
      List("Germany India USA","USA India Russia","India Brazil Canada China")
    )

    val wordsRdd = rdd.flatMap(_.split(" "))
    val pairRDD = wordsRdd.map(f=>(f,1))
    pairRDD.foreach(println)

    println("Distinct ==>")
    pairRDD.distinct().foreach(println)


    //SortByKey
    println("Sort by Key ==>")
    val sortRDD = pairRDD.sortByKey()
    sortRDD.foreach(println)

    //reduceByKey
    println("Reduce by Key ==>")
    val wordCount = pairRDD.reduceByKey((a,b)=>a+b)
    wordCount.foreach(println)

    def param1= (accu:Int,v:Int) => accu + v
    def param2= (accu1:Int,accu2:Int) => accu1 + accu2
    println("Aggregate by Key ==> wordcount")
    val wordCount2 = pairRDD.aggregateByKey(0)(param1,param2)
    wordCount2.foreach(println)

    //keys
    println("Keys ==>")
    wordCount2.keys.foreach(println)

    //values
    println("values ==>")
    wordCount2.values.foreach(println)

    println("Count :"+wordCount2.count())

    println("collectAsMap ==>")
    pairRDD.collectAsMap().foreach(println)

  }
}
