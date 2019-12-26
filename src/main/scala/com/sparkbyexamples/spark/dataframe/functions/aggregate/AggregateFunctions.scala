package com.sparkbyexamples.spark.dataframe.functions.aggregate

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AggregateFunctions extends App {

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  val simpleData = Seq(("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100)
  )
  val df = simpleData.toDF("employee_name", "department", "salary")
  df.show()

  //approx_count_distinct()
  println("approx_count_distinct: "+
    df.select(approx_count_distinct("salary")).collect()(0)(0))

  //avg
  println("avg: "+
    df.select(avg("salary")).collect()(0)(0))

  //collect_list

  df.select(collect_list("salary")).show(false)

  //collect_set

  df.select(collect_set("salary")).show(false)

  //countDistinct
  val df2 = df.select(countDistinct("department", "salary"))
  df2.show(false)
  println("Distinct Count of Department & Salary: "+df2.collect()(0)(0))

  println("count: "+
    df.select(count("salary")).collect()(0))

  //first
  df.select(first("salary")).show(false)

  //last
  df.select(last("salary")).show(false)

  //Exception in thread "main" org.apache.spark.sql.AnalysisException:
  // grouping() can only be used with GroupingSets/Cube/Rollup;
  //df.select(grouping("salary")).show(false)

  df.select(kurtosis("salary")).show(false)

  df.select(max("salary")).show(false)

  df.select(min("salary")).show(false)

  df.select(mean("salary")).show(false)

  df.select(skewness("salary")).show(false)

  df.select(stddev("salary"), stddev_samp("salary"),
    stddev_pop("salary")).show(false)

  df.select(sum("salary")).show(false)

  df.select(sumDistinct("salary")).show(false)

  df.select(variance("salary"),var_samp("salary"),
    var_pop("salary")).show(false)
}
