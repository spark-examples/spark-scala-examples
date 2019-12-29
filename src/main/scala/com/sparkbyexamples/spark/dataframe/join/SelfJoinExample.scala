package com.sparkbyexamples.spark.dataframe.join

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object SelfJoinExample extends App {

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val emp = Seq((1,"Smith",1,"10",3000),
    (2,"Rose",1,"20",4000),
    (3,"Williams",1,"10",1000),
    (4,"Jones",2,"10",2000),
    (5,"Brown",2,"40",-1),
    (6,"Brown",2,"50",-1)
  )
  val empColumns = Seq("emp_id","name","superior_emp_id","emp_dept_id","salary")
  import spark.sqlContext.implicits._
  val empDF = emp.toDF(empColumns:_*)
  empDF.show(false)

  println("self join")
  val selfDF = empDF.as("emp1").join(empDF.as("emp2"),
    col("emp1.superior_emp_id") === col("emp2.emp_id"),"inner")
  selfDF.show(false)

  selfDF.select(col("emp1.emp_id"),col("emp1.name"),
      col("emp2.emp_id").as("superior_emp_id"),
      col("emp2.name").as("superior_emp_name"))
    .show(false)

  //Spark SQL self join with where clause
  empDF.as("emp1").join(empDF.as("emp2")).where(
    col("emp1.superior_emp_id") === col("emp2.emp_id"))
  .select(col("emp1.emp_id"),col("emp1.name"),
    col("emp2.emp_id").as("superior_emp_id"),
    col("emp2.name").as("superior_emp_name"))
    .show(false)

  //Spark SQL self join with filter clause
  empDF.as("emp1").join(empDF.as("emp2")).filter(
    col("emp1.superior_emp_id") === col("emp2.emp_id"))
    .select(col("emp1.emp_id"),col("emp1.name"),
      col("emp2.emp_id").as("superior_emp_id"),
      col("emp2.name").as("superior_emp_name"))
    .show(false)


  empDF.createOrReplaceTempView("EMP")
  spark.sql("select emp1.emp_id,emp1.name," +
    "emp2.emp_id as superior_emp_id, emp2.name as superior_emp_name " +
    "from EMP emp1 INNER JOIN EMP emp2 on emp1.superior_emp_id == emp2.emp_id")
    .show(false)

}
