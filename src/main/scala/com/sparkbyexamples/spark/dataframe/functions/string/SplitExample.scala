package com.sparkbyexamples.spark.dataframe.functions.string

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, DataType, DataTypes}

object SplitExample extends App{

  val spark = SparkSession.builder()
    .appName("SparkByExamples.com")
    .master("local")
    .getOrCreate()

  val data = Seq(("James, A, Smith","2018","M",3000),
    ("Michael, Rose, Jones","2010","M",4000),
    ("Robert,K,Williams","2010","M",4000),
    ("Maria,Anne,Jones","2005","F",4000),
    ("Jen,Mary,Brown","2010","",-1)
  )

  import spark.sqlContext.implicits._
  val df = data.toDF("name","dob_year","gender","salary")
  df.printSchema()
  df.show(false)

  val df2 = df.select(split(col("name"),",").getItem(0).as("FirstName"),
    split(col("name"),",").getItem(1).as("MiddleName"),
    split(col("name"),",").getItem(2).as("LastName"))
    .drop("name")

  df2.printSchema()
  df2.show(false)


  val splitDF = df.withColumn("FirstName",split(col("name"),",").getItem(0))
    .withColumn("MiddleName",split(col("name"),",").getItem(1))
    .withColumn("LastName",split(col("name"),",").getItem(2))
    .withColumn("NameArray",split(col("name"),","))
    .drop("name")
  splitDF.printSchema()
  splitDF.show(false)

  df.createOrReplaceTempView("PERSON")
  spark.sql("select SPLIT(name,',') as NameArray from PERSON")
    .show(false)


  val splitDF2 = df.withColumn("FirstName",split(col("name"),",").getItem(0))
    .withColumn("MiddleName",array_join(slice(split(col("name"),","),2,3),"/"))

    .withColumn("NameArray",split(col("name"),","))
    .drop("name")
  splitDF2.printSchema()
  splitDF2.show(false)
}
