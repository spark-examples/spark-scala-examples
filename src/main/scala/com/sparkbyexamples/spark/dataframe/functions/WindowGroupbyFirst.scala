package com.sparkbyexamples.spark.dataframe.functions
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


object WindowGroupbyFirst extends App {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExamples.com")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val simpleData = Seq(("James","Sales",3000),
      ("Michael","Sales",4600),
      ("Robert","Sales",4100),
      ("Maria","Finance",3000),
      ("Raman","Finance",3000),
      ("Scott","Finance",3300),
      ("Jen","Finance",3900),
      ("Jeff","Marketing",3000),
      ("Kumar","Marketing",2000)
    )
    val df = simpleData.toDF("employee_name","department","salary")
    df.show()

    val w2 = Window.partitionBy("department").orderBy(col("salary"))
    df.withColumn("row",row_number.over(w2))
      .where($"row" === 1).drop("row")
      .show()

    val w3 = Window.partitionBy("department").orderBy(col("salary").desc)
    df.withColumn("row",row_number.over(w3))
      .where($"row" === 1).drop("row")
      .show()

    //Maximum, Minimum, Average salary for each window
    val w4 = Window.partitionBy("department")
    val aggDF = df.withColumn("row",row_number.over(w3))
      .withColumn("avg", avg(col("salary")).over(w4))
      .withColumn("sum", sum(col("salary")).over(w4))
      .withColumn("min", min(col("salary")).over(w4))
      .withColumn("max", max(col("salary")).over(w4))
      .where(col("row")===1).select("department","avg","sum","min","max")
      .show()

    /*val df = spark.sparkContext.parallelize(Seq(
      (0,"cat26",30.9), (0,"cat13",22.1), (0,"cat95",19.6), (0,"cat105",1.3),
      (1,"cat67",28.5), (1,"cat4",26.8), (1,"cat13",12.6), (1,"cat23",5.3),
      (2,"cat56",39.6), (2,"cat40",29.7), (2,"cat187",27.9), (2,"cat68",9.8),
      (3,"cat8",35.6))).toDF("Hour", "Category", "TotalValue")

    val w = Window.partitionBy($"hour").orderBy($"TotalValue".desc)

    val dfTop = df.withColumn("rn", row_number.over(w))
    dfTop.show()
    dfTop.where($"rn" === 1).drop("rn").show()

    //dfTop.show

    // ------------
    val dfMax = df.groupBy($"hour".as("max_hour")).agg(max($"TotalValue").as("max_value"))

    val dfTopByJoin = df.join(broadcast(dfMax),
      ($"hour" === $"max_hour") &amp;&amp; ($"TotalValue" === $"max_value"))
      .drop("max_hour")
      .drop("max_value")

    dfTopByJoin.show

    //-----------

    dfTopByJoin
      .groupBy($"hour")
      .agg(
        first("category").alias("category"),
        first("TotalValue").alias("TotalValue"))

    // ----------
    val win = Window.partitionBy($"col1", $"col2", $"col3").orderBy($"timestamp".desc)

    val refined_df = df.withColumn("rn", row_number.over(win)).where($"rn" === 1).drop("rn")


    //----------

    df.groupBy($"Hour")
      .agg(max(struct($"TotalValue", $"Category")).as("argmax"))
      .select($"Hour", $"argmax.*").show
      */


}
