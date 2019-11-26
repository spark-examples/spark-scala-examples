package com.sparkbyexamples.spark.dataframe

import javax.xml.transform.stream.StreamSource
import org.apache.spark.sql.{Encoders, Row, SparkSession}
import org.apache.spark.sql.types.{ArrayType, DataType, DataTypes, DoubleType, IntegerType, MapType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{col, from_json, struct, when}

import scala.io.Source

object StructTypeUsage extends App{

  /*
  StructType
  StructField
  ArrayType
  MapType
  schema
  DLL
  from json

   */
  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()
  import spark.implicits._
  /* Example 1 */
  val simpleData = Seq(Row("James ","","Smith","36636","M",3000),
    Row("Michael ","Rose","","40288","M",4000),
    Row("Robert ","","Williams","42114","M",4000),
    Row("Maria ","Anne","Jones","39192","F",4000),
    Row("Jen","Mary","Brown","","F",-1)
  )

  val simpleSchema = StructType(Array(
    StructField("firstname",StringType,true),
    StructField("middlename",StringType,true),
    StructField("lastname",StringType,true),
    StructField("id", StringType, true),
    StructField("gender", StringType, true),
    StructField("salary", IntegerType, true)
  ))

  val df = spark.createDataFrame(spark.sparkContext.parallelize(simpleData),simpleSchema)

  df.printSchema()
  df.show()


  /* Example 2
  * nested structure schema
  */

  val structureData = Seq(
    Row(Row("James ","","Smith"),"36636","M",3100),
    Row(Row("Michael ","Rose",""),"40288","M",4300),
    Row(Row("Robert ","","Williams"),"42114","M",1400),
    Row(Row("Maria ","Anne","Jones"),"39192","F",5500),
    Row(Row("Jen","Mary","Brown"),"","F",-1)
  )

  val structureSchema = new StructType()
    .add("name",new StructType()
      .add("firstname",StringType)
      .add("middlename",StringType)
      .add("lastname",StringType))
    .add("id",StringType)
    .add("gender",StringType)
    .add("salary",IntegerType)


  val df2 = spark.createDataFrame(spark.sparkContext.parallelize(structureData),structureSchema)
  df2.printSchema()
  df2.show()

  /* Schema from Json file */
  val url = ClassLoader.getSystemResource("schema.json")
  val schemaSource = Source.fromFile(url.getFile).getLines.mkString
  val schemaFromJson = DataType.fromJson(schemaSource).asInstanceOf[StructType]
  val df3 = spark.createDataFrame(spark.sparkContext.parallelize(structureData),schemaFromJson)
  //df3.printSchema()

  /* Using StructType case class*/
  val schemaFromCase = StructType(Array(
    StructField("name", StructType(Array(
      StructField("firstname",StringType,true),
      StructField("middlename",StringType,true),
      StructField("lastname",StringType,true))),true),
    StructField("id", StringType, true),
    StructField("gender", StringType, true),
    StructField("salary", IntegerType, true)
  ))
  val df4 = spark.createDataFrame(spark.sparkContext.parallelize(structureData),schemaFromCase)
  df4.printSchema()
  df4.show()

  /* Adding the structure of the DataFrame
  * Changing the structure of the DataFrame
  * */
  val updatedDF = df4.withColumn("OtherInfo", struct(  col("id").as("identifier"),
    col("gender").as("gender"),
    col("salary").as("salary"),
    when(col("salary").cast(IntegerType) < 2000,"Low")
      .when(col("salary").cast(IntegerType) < 4000,"Medium")
      .otherwise("High").alias("Salary_Grade")
  )).drop("id","gender","salary")

  updatedDF.printSchema()
  updatedDF.show(false)

  /* with List, array and maps*/

  val arrayStructureData = Seq(
    Row(Row("James ","","Smith"),List("Cricket","Movies"),Map("hair"->"black","eye"->"brown")),
    Row(Row("Michael ","Rose",""),List("Tennis"),Map("hair"->"brown","eye"->"black")),
    Row(Row("Robert ","","Williams"),List("Cooking","Football"),Map("hair"->"red","eye"->"gray")),
    Row(Row("Maria ","Anne","Jones"),null,Map("hair"->"blond","eye"->"red")),
    Row(Row("Jen","Mary","Brown"),List("Blogging"),Map("white"->"black","eye"->"black"))
  )

  val arrayStructureSchema = new StructType()
    .add("name",new StructType()
      .add("firstname",StringType)
      .add("middlename",StringType)
      .add("lastname",StringType))
    .add("Hobbies", ArrayType(StringType))
    .add("properties", MapType(StringType,StringType))

  val df5 = spark.createDataFrame(spark.sparkContext.parallelize(arrayStructureData),arrayStructureSchema)
  df5.printSchema()
  df5.show(false)

  /* How to check to schemas are same */

  /* Find if a column exists in schema */

  /* converting case class to Schema */
  case class Name(first:String,last:String,middle:String)
  case class Employee(fullName:Name,age:Integer,gender:String)

  import org.apache.spark.sql.catalyst.ScalaReflection
  val schema = ScalaReflection.schemaFor[Employee].dataType.asInstanceOf[StructType]

  val encoderSchema = Encoders.product[Employee].schema
  encoderSchema.printTreeString()

  /* Creating StructType schema from String DDL */
  val ddlSchemaStr = "`fullName` STRUCT<`first`: STRING, `last`: STRING, `middle`: STRING>,`age` INT,`gender` STRING"
  val ddlSchema = StructType.fromDDL(ddlSchemaStr)
  ddlSchema.printTreeString()

  /* check if a field exists in a DataFrame*/
  println(df.schema.fieldNames.contains("firstname"))
  println(df.schema.contains(StructField("firstname",StringType,true)))


  /* schema_of_json */

  /* Programmatically Specifying the Schema */

/*
  // https://stackoverflow.com/questions/45003393/spark-from-json-structtype-and-arraytype
  val schemaExample = new StructType()
    .add("FirstName", StringType)
    .add("Surname", StringType)
 // val dfExample2= spark.sql("""select "{\"A\":[{ \"FirstName\":\"Johnny\", \"Surname\":\"Boy\" }, { \"FirstName\":\"Franky\", \"Surname\":\"Man\" }]}" as theJson""")
 val dfExample2= spark.sql("""select "{ \"FirstName\":\"Johnny\", \"Surname\":\"Boy\" }, { \"FirstName\":\"Franky\", \"Surname\":\"Man\" }" as theJson""")
  //from_json(""" "{\"\":[{ \"FirstName\":\"Johnny\", \"Surname\":\"Boy\" }, { \"FirstName\":\"Franky\", \"Surname\":\"Man\" }]}" """, schemaExample)
  dfExample2.show(false)
  val dfICanWorkWith = dfExample2.select(from_json($"theJson", schemaExample).alias("cols")).select("cols.*")
  //println(dfICanWorkWith.collect())
  dfICanWorkWith.printSchema()
  dfICanWorkWith.show()
*/
}
