package com.sparkbyexamples.spark.stackoverflow

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
object Test {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExample")
      .getOrCreate();
    import spark.sqlContext.implicits._

    val df1:DataFrame  = Seq(
      ("Mark", "2018-02-20 00:00:00"),
      ("Alex", "2018-03-01 00:00:00"),
      ("Bob", "2018-03-01 00:00:00"),
      ("Mark", "2018-07-01 00:00:00"),
      ("Kate", "2018-07-01 00:00:00")
    ).toDF("USER_NAME", "REQUEST_DATE")

    df1.show()

    val df2: DataFrame  = Seq(
      ("Alex", "2018-01-01 00:00:00", "2018-02-01 00:00:00", "OUT"),
      ("Bob", "2018-02-01 00:00:00", "2018-02-05 00:00:00", "IN"),
      ("Mark", "2018-02-01 00:00:00", "2018-03-01 00:00:00", "IN"),
      ("Mark", "2018-05-01 00:00:00", "2018-08-01 00:00:00", "OUT"),
      ("Meggy", "2018-02-01 00:00:00", "2018-02-01 00:00:00", "OUT")
    ).toDF("NAME", "START_DATE", "END_DATE", "STATUS")

    df2.show()

    val df3 = df1.join(df2, col("USER_NAME") === col("NAME"), "left_outer")


    df3.groupBy("USER_NAME","REQUEST_DATE")

    val df4 = df3.withColumn("USER_STATUS", when($"REQUEST_DATE" > $"START_DATE" and $"REQUEST_DATE" < $"END_DATE", "Our user") otherwise ("Not our user"))

    df4.select("USER_NAME","REQUEST_DATE","USER_STATUS").distinct()show(false)
  }
}
