package com.sparkbyexamples.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object RDDFromParallelizeRange {
  def main(args: Array[String]): Unit = {

    val spark:SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("SparkByExample")
      .getOrCreate()

    val sc = spark.sparkContext

    val rdd4:RDD[Range] = sc.parallelize(List(1 to 1000))
    println("Number of Partitions : "+rdd4.getNumPartitions)

    val rdd5 = rdd4.repartition(5)
    println("Number of Partitions : "+rdd5.getNumPartitions)

    val rdd6:Array[Range] = rdd5.collect()
    println(rdd6.mkString(","))

    val rdd7:Array[Array[Range]] = rdd5.glom().collect()
    println("After glom");
    rdd7.foreach(f=>{
      println("For each partition")
      f.foreach(f1=>println(f1))
    })


  }

}
