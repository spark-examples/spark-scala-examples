package com.sparkbyexamples.spark.dataframe.examples

import org.apache.spark.sql.SparkSession

object CastStringToInt extends App {

  val spark = SparkSession.builder
      .master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()

  val simpleData = Seq(("James",34,"true","M","3000.6089"),
         ("Michael",33,"true","F","3300.8067"),
         ("Robert",37,"false","M","5000.5034")
     )

  import spark.implicits._
  val df = simpleData.toDF("firstname","age","isGraduated","gender","salary")
  df.printSchema()

  import org.apache.spark.sql.functions.col
  import org.apache.spark.sql.types.IntegerType
  // Convert String to Integer Type
  val df2= df.withColumn("salary",col("salary").cast(IntegerType))
  df2.printSchema()
  df2.show()

  df.withColumn("salary",col("salary").cast("int")).printSchema()
  df.withColumn("salary",col("salary").cast("integer")).printSchema()

  // Using select
  df.select(col("salary").cast("int").as("salary")).printSchema()

  //Using selectExpr()
  df.selectExpr("cast(salary as int) salary","isGraduated").printSchema()
  df.selectExpr("INT(salary)","isGraduated").printSchema()

  //Using with spark.sql()
  df.createOrReplaceTempView("CastExample")
  spark.sql("SELECT INT(salary),BOOLEAN(isGraduated),gender from CastExample").printSchema()
  spark.sql("SELECT cast(salary as int) salary, BOOLEAN(isGraduated),gender from CastExample").printSchema()
}
