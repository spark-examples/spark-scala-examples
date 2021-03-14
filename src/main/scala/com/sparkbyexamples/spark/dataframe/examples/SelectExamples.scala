package com.sparkbyexamples.spark.dataframe.examples

import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object SelectExamples extends App{

  val spark = SparkSession.builder
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  val data = Seq(("James","Smith","USA","CA"),
  ("Michael","Rose","USA","NY"),
  ("Robert","Williams","USA","CA"),
  ("Maria","Jones","USA","FL")
  )

  val columns = Seq("firstname","lastname","country","state")

  import spark.implicits._
  val df = data.toDF(columns:_*)
  df.show(false)


  df.select("firstname","lastname").show()
  //Using Dataframe object name
  df.select(df("firstname"),df("lastname")).show()
  //Using col function
  import org.apache.spark.sql.functions.col
  df.select(col("firstname"),col("lastname")).show()

  //Show all columns
  df.select("*").show()
  val columnsAll=df.columns.map(m=>col(m))
  df.select(columnsAll:_*).show()
  df.select(columns.map(m=>col(m)):_*).show()

  //Show columns from list
  val listCols= List("lastname","country")
  df.select(listCols.map(m=>col(m)):_*).show()

  //Show first few columns
  df.select(df.columns.slice(0,3).map(m=>col(m)):_*).show(1)

  //Show columns by index or position
  df.select(df.columns(3)).show(3)

  //Show columns from start and end index
  df.select(df.columns.slice(2,4).map(m=>col(m)):_*).show(3)

  //Show columns by regular expression
  df.select(df.colRegex("`^.*name*`")).show()

  df.select(df.columns.filter(f=>f.startsWith("first")).map(m=>col(m)):_*).show(3)
  df.select(df.columns.filter(f=>f.endsWith("name")).map(m=>col(m)):_*).show(3)

  //Show Nested columns
  val data2 = Seq(Row(Row("James","","Smith"),"OH","M"),
    Row(Row("Anna","Rose",""),"NY","F"),
    Row(Row("Julia","","Williams"),"OH","F"),
    Row(Row("Maria","Anne","Jones"),"NY","M"),
    Row(Row("Jen","Mary","Brown"),"NY","M"),
    Row(Row("Mike","Mary","Williams"),"OH","M")
  )

  val schema = new StructType()
    .add("name",new StructType()
      .add("firstname",StringType)
      .add("middlename",StringType)
      .add("lastname",StringType))
    .add("state",StringType)
    .add("gender",StringType)

  val df2 = spark.createDataFrame(
    spark.sparkContext.parallelize(data2),schema)
  df2.printSchema()
  df2.show(false)
  df2.select("name").show(false)
  df2.select("name.firstname","name.lastname").show(false)
  df2.select("name.*").show(false)
}
