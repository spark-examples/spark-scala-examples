package com.sparkbyexamples.spark.dataframe

import java.io.File

import org.apache.avro.Schema
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/**
  * Spark Avro library example
  * Avro schema example
  * Avro file format
  *
  */
object AvroExample {

  def main(args: Array[String]): Unit = {


    val spark: SparkSession = SparkSession.builder().master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()

    val data = Seq(("James ", "", "Smith", 2018, 1, "M", 3000),
      ("Michael ", "Rose", "", 2010, 3, "M", 4000),
      ("Robert ", "", "Williams", 2010, 3, "M", 4000),
      ("Maria ", "Anne", "Jones", 2005, 5, "F", 4000),
      ("Jen", "Mary", "Brown", 2010, 7, "", -1)
    )

    val columns = Seq("firstname", "middlename", "lastname", "dob_year",
      "dob_month", "gender", "salary")
    import spark.sqlContext.implicits._
    val df = data.toDF(columns: _*)

    /**
      * Write Avro File
      */
    df.write.format("avro")
      .mode(SaveMode.Overwrite)
      .save("C:\\tmp\\spark_out\\avro\\person.avro")

    /**
      * Read Avro File
      */
    spark.read.format("avro").load("C:\\tmp\\spark_out\\avro\\person.avro").show()

    /**
      * Write Avro Partition
      */
    df.write.partitionBy("dob_year","dob_month")
      .format("avro")
      .mode(SaveMode.Overwrite)
      .save("C:\\tmp\\spark_out\\avro\\person_partition.avro")

    /**
      * Reading Avro Partition
      */
    spark.read
      .format("avro")
      .load("C:\\tmp\\spark_out\\avro\\person_partition.avro")
      .where(col("dob_year") === 2010)
      .show()

    /**
      * Explicit Avro schema
      */
    val schemaAvro = new Schema.Parser()
      .parse(new File("src/main/resources/person.avsc"))

    spark.read
      .format("avro")
      .option("avroSchema", schemaAvro.toString)
      .load("C:\\tmp\\spark_out\\avro\\person.avro")
      .show()

    /**
      * Avro Spark SQL
      */
    spark.sqlContext.sql("CREATE TEMPORARY VIEW PERSON USING avro OPTIONS (path \"C:/tmp/spark_out/avro/person.avro\")")
    spark.sqlContext.sql("SELECT * FROM PERSON").show()
  }
}
