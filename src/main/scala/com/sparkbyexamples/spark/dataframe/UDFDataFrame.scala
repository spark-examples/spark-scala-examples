package com.sparkbyexamples.spark.dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object UDFDataFrame {
  def main(args:Array[String]): Unit = {

    val spark:SparkSession = SparkSession.builder()
      .master("local[3]")
      .appName("SparkByExample")
      .getOrCreate()

    val data = Seq(("2018/01/23",23),("2018/01/24",24),("2018/02/20",25))

    import spark.sqlContext.implicits._
    val df = data.toDF("date1","day")

    val replace: String => String = _.replace("/","-")
    import org.apache.spark.sql.functions.udf
    val replaceUDF = udf(replace)
    val minDate = df.agg(min($"date1")).collect()(0).get(0)

    val df2 = df.select("*").filter( to_date(replaceUDF($"date1")) > date_add(to_date(replaceUDF(lit(minDate))),7 ))
    df2.show()
  }


}
