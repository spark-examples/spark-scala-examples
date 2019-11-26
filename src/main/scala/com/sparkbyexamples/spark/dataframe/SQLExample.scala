package com.sparkbyexamples.spark.dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object DataFrameWithSQL_ {

  def main(args:Array[String]):Unit= {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()

    val data = Seq(1,2,3)

    import spark.sqlContext.implicits._

    val df = data.toDF("field1")

   df.createOrReplaceTempView("table1")

    val df2 = spark.sql("select tb1.field1 as field1,tb2.field1 as field2 from table1 tb1, table1 tb2 where tb1.field1 <> tb2.field1")
    df2.printSchema()
    df2.show(false)

    df2.createOrReplaceTempView("table2")

    val df3 = spark.sql("select distinct tb1.field1,tb1.field2 from table2 tb1, table2 tb2 where tb1.field1 == tb2.field2 and tb1.field2 == tb2.field1")

    df3.show(false)



  }
}
