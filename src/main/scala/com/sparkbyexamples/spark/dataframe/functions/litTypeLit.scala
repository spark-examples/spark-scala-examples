package com.sparkbyexamples.spark.dataframe.functions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType

object litTypeLit extends App {



  val spark = SparkSession.builder()
    .appName("sparkbyexamples.com")
    .master("local")
    .getOrCreate()

  import spark.sqlContext.implicits._
  import org.apache.spark.sql.functions._

  val data = Seq(("111",50000),("222",60000),("333",40000))
  val df = data.toDF("EmpId","Salary")
  val df2 = df.select(col("EmpId"),col("Salary"),lit("1").as("lit_value1"))
  df2.show()

  val df3 = df2.withColumn("lit_value2",
    when(col("Salary") >=40000 && col("Salary") <= 50000, lit("100").cast(IntegerType))
      .otherwise(lit("200").cast(IntegerType))
  )

  df3.show()

  val df4 = df3.withColumn("typedLit_seq",typedLit(Seq(1, 2, 3)))
    .withColumn("typedLit_map",typedLit(Map("a" -> 1, "b" -> 2)))
    .withColumn("typedLit_struct",typedLit(("a", 2, 1.0)))

  df4.printSchema()
  df4.show()

}
