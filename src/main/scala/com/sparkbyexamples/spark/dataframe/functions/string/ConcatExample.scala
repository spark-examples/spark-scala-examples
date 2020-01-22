package com.sparkbyexamples.spark.dataframe.functions.string

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{lit, _}
object ConcatExample extends App {

  val spark = SparkSession.builder()
    .appName("SparkByExamples.com")
    .master("local")
    .getOrCreate()

  val data = Seq(("James","A","Smith","2018","M",3000),
    ("Michael","Rose","Jones","2010","M",4000),
    ("Robert","K","Williams","2010","M",4000),
    ("Maria","Anne","Jones","2005","F",4000),
    ("Jen","Mary","Brown","2010","",-1)
  )

  val columns = Seq("fname","mname","lname","dob_year","gender","salary")
  import spark.sqlContext.implicits._
  val df = data.toDF(columns:_*)
  df.printSchema()
  df.show(false)

  df.select(concat(col("fname"),lit(','),
    col("mname"),lit(','),col("lname")).as("FullName"))
      .show(false)

  df.withColumn("FullName",concat(col("fname"),lit(','),
    col("mname"),lit(','),col("lname")))
    .drop("fname")
    .drop("mname")
    .drop("lname")
    .show(false)

  df.withColumn("FullName",concat_ws(",",col("fname"),col("mname"),col("lname")))
    .drop("fname")
    .drop("mname")
    .drop("lname")
      .show(false)

  df.createOrReplaceTempView("EMP")

  spark.sql("select CONCAT(fname,' ',lname,' ',mname) as FullName from EMP")
    .show(false)

  spark.sql("select fname ||' '|| lname ||' '|| mname as FullName from EMP")
    .show(false)
}
