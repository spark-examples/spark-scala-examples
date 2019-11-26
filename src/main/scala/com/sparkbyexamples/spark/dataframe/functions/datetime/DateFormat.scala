package com.sparkbyexamples.spark.dataframe.functions.datetime

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DateFormat extends App {

  val spark:SparkSession = SparkSession.builder()
    .master("local")
    .appName("SparkByExamples.com")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  import spark.sqlContext.implicits._

  //Current Date and Time stamp
  // Use <code>current_date</code> function to get current system date in Spark DateType
  // `yyyy-MM-dd format and <code>current_timestamp</code> to get current time stamp in `yyyy-MM-dd HH:mm:ss.SSSS`
  Seq(1).toDF("seq").select(
    current_date().as("current_date"), //returns date in `yyyy-MM-dd'
    current_timestamp().as("current_time") //returns date and time in `yyyy-MM-dd HH:mm:ss.SSSS`
  ).show(false)

  //Format Date to String
  //Let's convert the date and time from Spark DateType format to different formats
  Seq(1).toDF("seq").select(
    current_date().as("current_date"),
    date_format(current_timestamp(),"yyyy MM dd").as("yyyy MM dd"),
    date_format(current_timestamp(),"MM/dd/yyyy hh:mm").as("MM/dd/yyyy"),
    date_format(current_timestamp(),"yyyy MMM dd").as("yyyy MMMM dd"),
    date_format(current_timestamp(),"yyyy MMMM dd E").as("yyyy MMMM dd E")
  ).show(false)

  //Format Date String to Date
  //let's see another example of how to convert string date to Spark DateType format.
  // when dates are in 'yyyy-MM-dd' format, spark function <code>to_date</code> auto cast
  // to DateType by casting rules. When dates are in different format this function returns null.
  Seq(("2019-07-24"),("07-24-2009")).toDF("Date").select(
    col("Date"),
    to_date(col("Date")).as("to_date")
  ).show()

  //However, Spark defined overloaded function of to_date which takes format of the input string as additional parameter.
  // Using this we should able to convert any date string to Spark DateType.
  Seq(("06-03-2009"),("07-24-2009")).toDF("Date").select(
    col("Date"),
    to_date(col("Date"),"MM-dd-yyyy").as("to_date")
  ).show()

  // Format Different Date Strings to Spark DateType
  //Below examples shows how to convert different dates from a single column to Spark DateType using <code>to_date</code> function.
  Seq(("2019-07-24"),("07/24/2019"),("2019 Jul 24"),("07-27-19"))
    .toDF("Date").select(
    col("Date"),
    when(to_date(col("Date"),"yyyy-MM-dd").isNotNull,
      to_date(col("Date"),"yyyy-MM-dd"))
    .when(to_date(col("Date"),"MM/dd/yyyy").isNotNull,
      to_date(col("Date"),"MM/dd/yyyy"))
    .when(to_date(col("Date"),"yyyy MMMM dd").isNotNull,
      to_date(col("Date"),"yyyy MMMM dd"))
    .otherwise("Unknown Format").as("Formated Date")
  ).show()

  //Handling Different Date String to Specific Date Format
  //Below examples shows how to convert different dates from a single column
  // to standard date string using <code>to_date</code> and <code>date_format</code> function.
  Seq(("2019-07-24"),("07/24/2019"),("2019 Jul 24"),("07-27-19"))
    .toDF("Date").select(
    col("Date"),
    when(to_date(col("Date"),"yyyy-MM-dd").isNotNull,
      date_format(to_date(col("Date"),"yyyy-MM-dd"),"MM/dd/yyyy"))
    .when(to_date(col("Date"),"MM/dd/yyyy").isNotNull,
      date_format(to_date(col("Date"),"MM/dd/yyyy"),"MM/dd/yyyy"))
    .when(to_date(col("Date"),"yyyy MMMM dd").isNotNull,
      date_format(to_date(col("Date"),"yyyy MMMM dd"),"MM/dd/yyyy"))
    .otherwise("Unknown Format").as("Formated Date")
  ).show()


}
