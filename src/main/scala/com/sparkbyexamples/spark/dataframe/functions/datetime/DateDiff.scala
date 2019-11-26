package com.sparkbyexamples.spark.dataframe.functions.datetime

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object DateDiff extends App {

  val spark:SparkSession = SparkSession.builder()
    .master("local")
    .appName("SparkByExamples.com")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  import spark.sqlContext.implicits._

  //Difference between two dates in days
  Seq(("2019-07-01"),("2019-06-24"),("2019-08-24"),("2018-07-23")).toDF("date")
    .select(
      col("date"),
      current_date().as("current_date"),
      datediff(current_date(),col("date")).as("datediff")
    ).show()

  // Difference between two dates in Months and Years
  val df = Seq(("2019-07-01"),("2019-06-24"),("2019-08-24"),("2018-12-23"),("2018-07-20"))
    .toDF("startDate").select(
    col("startDate"),current_date().as("endDate")
  )

  calculateDiff(df)

  //Difference between two dates when dates are not in Spark DateType format 'yyyy-MM-dd'.
  //Note that when dates are not in Spark DateType format, all Spark functions returns null
  //Hence, first convert the input dates to Spark DateType using to_date function
  val dfDate = Seq(("07-01-2019"),("06-24-2019"),("08-24-2019"),("12-23-2018"),("07-20-2018"))
    .toDF("startDate").select(
    to_date(col("startDate"),"MM-dd-yyyy").as("startDate"),
    current_date().as("endDate")
  )

  calculateDiff(dfDate)

  def calculateDiff(df:DataFrame): Unit ={
    df.withColumn("datesDiff", datediff(col("endDate"),col("startDate")))
      .withColumn("montsDiff", months_between(
        col("endDate"),col("startDate")))
      .withColumn("montsDiff_round",round(months_between(
        col("endDate"),col("startDate")),2))
      .withColumn("yearsDiff",months_between(
        col("endDate"),col("startDate"),true).divide(12))
      .withColumn("yearsDiff_round",round(months_between(
        col("endDate"),col("startDate"),true).divide(12),2))
      .show()
  }
}
