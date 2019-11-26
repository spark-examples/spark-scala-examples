package com.sparkbyexamples.spark.dataframe.functions.datetime

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{last_day, _}

object DateTrunc extends App {


  val spark:SparkSession = SparkSession.builder()
    .master("local")
    .appName("SparkByExamples.com")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  import spark.sqlContext.implicits._

  // Truncate on Date
  Seq("1").toDF("date").select(
    current_timestamp(),
    trunc(current_timestamp(),"Year").as("Year"),
    trunc(current_timestamp(),"Month").as("Month"),
    trunc(current_timestamp(),"Day").as("Day") // returns null
  ).show(false)

  //Truncate on Date & Time
  Seq("1").toDF("date").select(
    current_timestamp(),
    date_trunc("Year",current_timestamp()).as("Year"),
    date_trunc("Month",current_timestamp()).as("Month"),
    date_trunc("Day",current_timestamp()).as("Day"),
    date_trunc("Hour",current_timestamp()).as("Hour"),
    date_trunc("Minute",current_timestamp()).as("Minute")
  ).show(false)

  //Truncate on Date & Time from input String Date
  Seq(("2019-01-23"),("2019-06-24"),("2019-09-20")).toDF("date").select(
    col("date"),
    trunc(col("date"),"Month").as("Month"),
    trunc(col("date"),"Year").as("Year")
  ).show()

  //Truncate on Date & Time from input String Date
  Seq(("01-23-2019"),("06-24-2019"),("09-20-2019")).toDF("date").select(
    col("date"),
    trunc(to_date(col("date"),"MM-dd-yyyy"),"Month").as("Month"),
    trunc(to_date(col("date"),"MM-dd-yyyy"),"Year").as("Year")
  ).show()


  //Beginning of the Year
  Seq("1").toDF("date").select(
    current_timestamp(),
    trunc(current_timestamp(),"Year").as("Year_Begin")
  ).show(false)

  //Beginning of the Month
  Seq("1").toDF("date").select(
    current_timestamp(),
    trunc(current_timestamp(),"Month").as("Month_Begin")
  ).show(false)

  //End of the Month
  Seq("1").toDF("date").select(
    current_timestamp(),
    last_day(current_timestamp).as("Month_End")
  ).show(false)

  //Beginning of the Next Month
  Seq("1").toDF("date").select(
    current_timestamp(),
    trunc(add_months(current_timestamp,1),"Month")
      .as("Next_Month_Begin")
  ).show(false)

  //Beginning of the Previous Month
  Seq("1").toDF("date").select(
    current_timestamp(),
    trunc(add_months(current_timestamp,-1),"Month")
      .as("Previous_Month_Begin")
  ).show(false)
}
