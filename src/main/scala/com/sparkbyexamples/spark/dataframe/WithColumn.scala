package com.sparkbyexamples.spark.dataframe

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{ArrayType, IntegerType, MapType, StringType, StructType}
import org.apache.spark.sql.functions._
object WithColumn {

  def main(args:Array[String]):Unit= {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()

    val arrayStructureData = Seq(
      Row(Row("James ","","Smith"),"1","M",3100,List("Cricket","Movies"),Map("hair"->"black","eye"->"brown")),
      Row(Row("Michael ","Rose",""),"2","M",3100,List("Tennis"),Map("hair"->"brown","eye"->"black")),
      Row(Row("Robert ","","Williams"),"3","M",3100,List("Cooking","Football"),Map("hair"->"red","eye"->"gray")),
      Row(Row("Maria ","Anne","Jones"),"4","M",3100,null,Map("hair"->"blond","eye"->"red")),
      Row(Row("Jen","Mary","Brown"),"5","M",3100,List("Blogging"),Map("white"->"black","eye"->"black"))
    )

    val arrayStructureSchema = new StructType()
      .add("name",new StructType()
        .add("firstname",StringType)
        .add("middlename",StringType)
        .add("lastname",StringType))
      .add("id",StringType)
      .add("gender",StringType)
      .add("salary",IntegerType)
      .add("Hobbies", ArrayType(StringType))
      .add("properties", MapType(StringType,StringType))

    val df2 = spark.createDataFrame(
      spark.sparkContext.parallelize(arrayStructureData),arrayStructureSchema)

    //Change the column data type
    df2.withColumn("salary",df2("salary").cast("Integer"))

    //Derive a new column from existing
    val df4=df2.withColumn("CopiedColumn",df2("salary")* -1)

    //Transforming existing column
    val df5 = df2.withColumn("salary",df2("salary")*100)

    //You can also chain withColumn to change multiple columns

    //Renaming a column.
    val df3=df2.withColumnRenamed("gender","sex")
    df3.printSchema()

    //Droping a column
    val df6=df4.drop("CopiedColumn")
    println(df6.columns.contains("CopiedColumn"))

    //Adding a literal value
    df2.withColumn("Country", lit("USA")).printSchema()

    //Retrieving
    df2.show(false)
    df2.select("name").show(false)
    df2.select("name.firstname").show(false)
    df2.select("name.*").show(false)


    val df8 = df2.select(col("*"),explode(col("hobbies")))
    df8.show(false)

    import spark.implicits._

    val columns = Seq("name","address")
    val data = Seq(("Robert, Smith", "1 Main st, Newark, NJ, 92537"), ("Maria, Garcia","3456 Walnut st, Newark, NJ, 94732"))
    var dfFromData = spark.createDataFrame(data).toDF(columns:_*)
    dfFromData.printSchema()

    val newDF = dfFromData.map(f=>{
      val nameSplit = f.getAs[String](0).split(",")
      val addSplit = f.getAs[String](1).split(",")
      (nameSplit(0),nameSplit(1),addSplit(0),addSplit(1),addSplit(2),addSplit(3))
    })
    val finalDF = newDF.toDF("First Name","Last Name","Address Line1","City","State","zipCode")
    finalDF.printSchema()
    finalDF.show(false)
  }
}
