package com.sparkbyexamples.spark.dataframe.functions.datetime

import com.sparkbyexamples.spark.dataframe.functions.datetime.DateFormat.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{current_date, current_timestamp, date_format}

object DateToString extends App {

  val spark:SparkSession = SparkSession.builder()
    .master("local")
    .appName("SparkByExamples.com")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  import spark.sqlContext.implicits._

  Seq(1).toDF("seq").select(
    current_date().as("current_date"),
    date_format(current_timestamp(),"yyyy MM dd").as("yyyy MM dd"),
    date_format(current_timestamp(),"MM/dd/yyyy hh:mm").as("MM/dd/yyyy"),
    date_format(current_timestamp(),"yyyy MMM dd").as("yyyy MMMM dd"),
    date_format(current_timestamp(),"yyyy MMMM dd E").as("yyyy MMMM dd E")
  ).show(false)

}
