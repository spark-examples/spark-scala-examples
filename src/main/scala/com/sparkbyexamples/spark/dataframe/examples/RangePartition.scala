package com.sparkbyexamples.spark.dataframe.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
object RangePartition extends App{

  val spark: SparkSession = SparkSession.builder()  .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  /**
    * Simple using columns list
    */
  val data = Seq((1,10),(2,20),(3,10),(4,20),(5,10),
    (6,30),(7,50),(8,50),(9,50),(10,30),
    (11,10),(12,10),(13,40),(14,40),(15,40),
    (16,40),(17,50),(18,10),(19,40),(20,40)
  )

  import spark.sqlContext.implicits._
  val dfRange = data.toDF("id","count")
               .repartitionByRange(5,col("count"))

  dfRange.write.option("header",true).csv("c:/tmp/range-partition")
  dfRange.write.partitionBy()

}
