package com.sparkbyexamples.spark.dataframe

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.functions._
object WithColumn {

  def main(args:Array[String]):Unit= {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()

    val dataRows = Seq(Row(Row("James;","","Smith"),"36636","M","3000"),
      Row(Row("Michael","Rose",""),"40288","M","4000"),
      Row(Row("Robert","","Williams"),"42114","M","4000"),
      Row(Row("Maria","Anne","Jones"),"39192","F","4000"),
      Row(Row("Jen","Mary","Brown"),"","F","-1")
    )

    val schema = new StructType()
      .add("name",new StructType()
        .add("firstname",StringType)
        .add("middlename",StringType)
        .add("lastname",StringType))
      .add("dob",StringType)
      .add("gender",StringType)
      .add("salary",StringType)

    val df2 = spark.createDataFrame(spark.sparkContext.parallelize(dataRows),schema)

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

    df2.createOrReplaceTempView("PERSON")
    spark.sql("SELECT salary*100 as salary, salary*-1 as CopiedColumn, 'USA' as country FROM PERSON").show()
  }

}
