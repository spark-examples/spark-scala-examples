package com.sparkbyexamples.spark.dataframe.functions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, typedLit, when}
import org.apache.spark.sql.types.IntegerType

object AddColumn extends App {

  val spark = SparkSession.builder()
    .appName("SparkByExamples.com")
    .master("local")
    .getOrCreate()

  import spark.sqlContext.implicits._

  val data = Seq(("111",50000),("222",60000),("333",40000))
  val df = data.toDF("EmpId","Salary")
  df.show(false)

  //Derive a new column from existing
  df.withColumn("CopiedColumn",df("salary")* -1)
    .show(false)

  //Using select
  df.select($"EmpId",$"Salary", ($"salary"* -1).as("CopiedColumn") )
    .show(false)

  //Adding a literal
  val df2 = df.select(col("EmpId"),col("Salary"),lit("1").as("lit_value1"))
  df2.show()

  val df3 = df2.withColumn("lit_value2",
    when(col("Salary") >=40000 && col("Salary") <= 50000, lit("100").cast(IntegerType))
      .otherwise(lit("200").cast(IntegerType))
  )
  df3.show(false)

  //Adding a list column
  val df4 = df3.withColumn("typedLit_seq",typedLit(Seq(1, 2, 3)))
    .withColumn("typedLit_map",typedLit(Map("a" -> 1, "b" -> 2)))
    .withColumn("typedLit_struct",typedLit(("a", 2, 1.0)))

  df4.printSchema()
  df4.show()


}
