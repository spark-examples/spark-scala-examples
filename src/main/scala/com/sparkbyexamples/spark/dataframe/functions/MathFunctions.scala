package com.sparkbyexamples.spark.dataframe.functions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object MathFunctions {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("sparkbyexamples.com")
      .master("local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    val data = Seq((2,2.67),(3,3.12),(4,4.34),(5,1.10))
   // data.sc
//    data.printSchema()
//    data.withColumn("factorial",factorial(col("number")))
//     // .withColumn("ceil")
//      .show()

  }
}
