package com.sparkbyexamples.spark.dataframe.functions.datetime

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object DateAddMonths extends App {

  val spark:SparkSession = SparkSession.builder()
    .master("local")
    .appName("SparkByExamples.com")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  import spark.sqlContext.implicits._

  Seq(("2019-01-23"),("2019-06-24"),("2019-09-20")).toDF("date").select(
    col("date"),
    add_months(col("date"),3).as("add_months"),
    add_months(col("date"),-3).as("sub_months"),
    date_add(col("date"),4).as("date_add"),
    date_sub(col("date"),4).as("date_sub")
  ).show()

  Seq(("06-03-2009"),("07-24-2009")).toDF("date").select(
    col("Date"),
    add_months(to_date(col("Date"),"MM-dd-yyyy"),3).as("add_months"),
    add_months(to_date(col("Date"),"MM-dd-yyyy"),-3).as("add_months2"),
    date_add(to_date(col("Date"),"MM-dd-yyyy"),3).as("date_add"),
      date_add(to_date(col("Date"),"MM-dd-yyyy"),-3).as("date_add2"),
    date_sub(to_date(col("Date"),"MM-dd-yyyy"),3).as("date_sub")
  ).show()

//  Seq(("2019-01-23",1),("2019-06-24",2),("2019-09-20",3)).toDF("date","increment").select(
//    col("date"),
//  add_months(to_date(col("date"),"yyyy-MM-dd"),col("increment").cast(IntegerType).).as("date_inc")
//  ).show()

  Seq(("2019-01-23",1),("2019-06-24",2),("2019-09-20",3))
    .toDF("date","increment")
    .select(col("date"),col("increment"),
      expr("add_months(to_date(date,'yyyy-MM-dd'),cast(increment as int))").as("inc_date"))
    .show()

  Seq(("2019-01-23",1),("2019-06-24",2),("2019-09-20",3))
    .toDF("date","increment")
    .selectExpr("date","increment","add_months(to_date(date,'yyyy-MM-dd'),cast(increment as int)) as inc_date")
    .show()
}
