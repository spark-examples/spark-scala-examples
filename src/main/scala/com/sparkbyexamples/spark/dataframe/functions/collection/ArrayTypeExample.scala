package com.sparkbyexamples.spark.dataframe.functions.collection

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

//https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/142158605138935/3773509768457258/7497868276316206/latest.html

object ArrayTypeExample extends App {

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()
  import spark.implicits._

  val arrayStructureData = Seq(
    Row("James,,Smith",List("Java","Scala","C++"),List("Spark","Java"),"OH","CA"),
    Row("Michael,Rose,",List("Spark","Java","C++"),List("Spark","Java"),"NY","NJ"),
    Row("Robert,,Williams",List("CSharp","VB"),List("Spark","Python"),"UT","NV")
  )

  val arrayStructureSchema = new StructType()
    .add("name",StringType)
    .add("languagesAtSchool", ArrayType(StringType))
    .add("languagesAtWork", ArrayType(StringType))
    .add("currentState", StringType)
    .add("previousState", StringType)

  val df = spark.createDataFrame(
    spark.sparkContext.parallelize(arrayStructureData),arrayStructureSchema)
  df.printSchema()
  df.show()

  df.select($"name",explode($"languagesAtSchool")).show(false)

  df.select(split($"name",",").as("nameAsArray") ).show(false)

  df.select($"name",array($"currentState",$"previousState").as("States") ).show(false)

  df.select($"name",array_contains($"languagesAtSchool","Java")
    .as("array_contains")).show(false)

  df.select($"name",array_union($"languagesAtSchool",$"languagesAtWork")
    .as("array_union")).show(false)

  //df.select($"name",array_distinct($"languagesAtSchool")).show(false)

  df.select($"name",array_intersect($"languagesAtSchool",$"languagesAtWork")).show(false)

  df.select($"name",array_except($"languagesAtSchool",$"languagesAtWork"),
    array_except($"languagesAtWork",$"languagesAtSchool")).show(false)

  df.select($"name",array_join($"languagesAtSchool","|")).show(false)

  df.select($"name",array_repeat($"name",3)).show(false)

  df.select($"name",array_remove($"languagesAtSchool","Java")).show(false)

  df.select($"name",array_sort($"languagesAtSchool")).show(false)

  df.select($"name",arrays_overlap($"languagesAtSchool",$"languagesAtWork")).show(false)

  df.select($"name",arrays_zip($"languagesAtSchool",$"languagesAtWork")).show(false)

  df.select($"name",concat($"languagesAtSchool",$"languagesAtWork")).show(false)

  df.select($"name",array_distinct(concat($"languagesAtSchool",$"languagesAtWork"))).show(false)

  df.select($"name",$"languagesAtSchool",reverse($"languagesAtSchool")).show(false)

  df.select($"name",element_at($"languagesAtSchool",3),$"languagesAtSchool".getItem(2)).show(false)

  df.select($"name",sequence(lit("1").cast(IntegerType),lit("10").cast(IntegerType))).show(false)
  df.select($"name",shuffle($"languagesAtSchool")).show(false)

  df.select($"name",slice($"languagesAtSchool",1,2)).show(false)

  //How to flatten Array of Array
  val arrayArrayData = Seq(
    Row("James",List(List("Java","Scala","C++"),List("Spark","Java"))),
    Row("Michael",List(List("Spark","Java","C++"),List("Spark","Java"))),
    Row("Robert",List(List("CSharp","VB"),List("Spark","Python")))
  )

  val arrayArraySchema = new StructType().add("name",StringType)
    .add("subjects",ArrayType(ArrayType(StringType)))

  val df3 = spark.createDataFrame(spark.sparkContext.parallelize(arrayArrayData),arrayArraySchema)
  df3.printSchema()
  df3.show()

  df3.select($"name",flatten($"subjects")).show(false)

  //Working with collect_list() and collect_set()
  val simpleData = Seq(("James","Sales",3000),
    ("Michael","Sales",4600),
    ("Robert","Sales",4100),
    ("Maria","Finance",3000),
    ("Jen","Finance",3000),
    ("Jen","Finance",3300),
    ("Jen","Finance",3900),
    ("Jen","Marketing",3000),
    ("Jen","Marketing",2000)
  )
  val df2 = simpleData.toDF("Name","Department","Salary")

  df2.groupBy($"Department").agg(collect_list($"Name")).show(false)

  import spark.implicits._

}
