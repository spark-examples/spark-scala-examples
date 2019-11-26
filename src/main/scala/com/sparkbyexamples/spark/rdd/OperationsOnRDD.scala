package com.sparkbyexamples.spark.rdd

import org.apache.spark.sql.SparkSession

object OperationsOnRDD {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("SparkByExample")
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val rdd = spark.sparkContext.parallelize(
      List("Germany India USA","USA London Russia","Mexico Brazil Canada China")
    )

    val listRdd = spark.sparkContext.parallelize(List(9,2,3,4,5,6,7,8))

    //reduce
    println("Minimum :"+listRdd.reduce((a,b)=> a min b))
    println("Maximum :"+listRdd.reduce((a,b)=> a max b))
    println("Sum :"+listRdd.reduce((a,b)=> a + b))

    //flatMap
    val wordsRdd = rdd.flatMap(_.split(" "))
    wordsRdd.foreach(println)

    //sortBy
    println("Sort by word name")
    val sortRdd = wordsRdd.sortBy(f=>f) // also can write f=>f

    //GroupBy
    val groupRdd = wordsRdd.groupBy(word=>word.length)
    groupRdd.foreach(println)

    //map
    val tupp2Rdd = wordsRdd.map(f=>(f,1))
    tupp2Rdd.foreach(println)


  }
}
