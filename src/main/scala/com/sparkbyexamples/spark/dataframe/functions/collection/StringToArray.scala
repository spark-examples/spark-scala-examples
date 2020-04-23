package com.sparkbyexamples.spark.dataframe.functions.collection

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, split}

object StringToArray extends App {

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

  val df2 = df.select(split(col("name"),",").as("NameArray"))
    .drop("name")

  df2.printSchema()
  df2.show(false)

  df.createOrReplaceTempView("PERSON")
  spark.sql("select SPLIT(name,',') as NameArray from PERSON")
    .show(false)

}
