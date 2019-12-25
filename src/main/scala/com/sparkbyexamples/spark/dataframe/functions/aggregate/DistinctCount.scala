package com.sparkbyexamples.spark.dataframe.functions.aggregate

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DistinctCount extends App {

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

  println("Distinct Count: " + df.distinct().count())

  val df2 = df.select(countDistinct("department", "salary"))
  df2.show(false)
  println("Distinct Count of Department & Salary: "+df2.collect()(0)(0))

}