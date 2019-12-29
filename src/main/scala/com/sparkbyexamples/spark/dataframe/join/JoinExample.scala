package com.sparkbyexamples.spark.dataframe.join

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
object JoinExample extends App {

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val emp = Seq((1,"Smith",-1,"2018","10","M",3000),
    (2,"Rose",1,"2010","20","M",4000),
    (3,"Williams",1,"2010","10","M",1000),
    (4,"Jones",2,"2005","10","F",2000),
    (5,"Brown",2,"2010","40","",-1),
      (6,"Brown",2,"2010","50","",-1)
  )
  val empColumns = Seq("emp_id","name","superior_emp_id","year_joined","emp_dept_id","gender","salary")
  import spark.sqlContext.implicits._
  val empDF = emp.toDF(empColumns:_*)
  empDF.show(false)

  val dept = Seq(("Finance",10),
    ("Marketing",20),
    ("Sales",30),
    ("IT",40)
  )

  val deptColumns = Seq("dept_name","dept_id")
  val deptDF = dept.toDF(deptColumns:_*)
  deptDF.show(false)


  println("Inner join")
  empDF.join(deptDF,empDF("emp_dept_id") ===  deptDF("dept_id"),"inner")
    .show(false)

  println("Outer join")
  empDF.join(deptDF,empDF("emp_dept_id") ===  deptDF("dept_id"),"outer")
    .show(false)
  println("full join")
  empDF.join(deptDF,empDF("emp_dept_id") ===  deptDF("dept_id"),"full")
    .show(false)
  println("fullouter join")
  empDF.join(deptDF,empDF("emp_dept_id") ===  deptDF("dept_id"),"fullouter")
    .show(false)

  println("right join")
  empDF.join(deptDF,empDF("emp_dept_id") ===  deptDF("dept_id"),"right")
    .show(false)
  println("rightouter join")
  empDF.join(deptDF,empDF("emp_dept_id") ===  deptDF("dept_id"),"rightouter")
    .show(false)

  println("left join")
  empDF.join(deptDF,empDF("emp_dept_id") ===  deptDF("dept_id"),"left")
    .show(false)
  println("leftouter join")
  empDF.join(deptDF,empDF("emp_dept_id") ===  deptDF("dept_id"),"leftouter")
    .show(false)

  println("leftanti join")
  empDF.join(deptDF,empDF("emp_dept_id") ===  deptDF("dept_id"),"leftanti")
    .show(false)

  println("leftsemi join")
  empDF.join(deptDF,empDF("emp_dept_id") ===  deptDF("dept_id"),"leftsemi")
    .show(false)

  println("cross join")
  empDF.join(deptDF,empDF("emp_dept_id") ===  deptDF("dept_id"),"cross")
    .show(false)

  println("Using crossJoin()")
  empDF.crossJoin(deptDF).show(false)

  println("self join")
  empDF.as("emp1").join(empDF.as("emp2"),
    col("emp1.superior_emp_id") === col("emp2.emp_id"),"inner")
    .select(col("emp1.emp_id"),col("emp1.name"),
      col("emp2.emp_id").as("superior_emp_id"),
      col("emp2.name").as("superior_emp_name"))
      .show(false)

  empDF.createOrReplaceTempView("EMP")
  deptDF.createOrReplaceTempView("DEPT")

  //SQL JOIN
  val joinDF = spark.sql("select * from EMP e, DEPT d where e.emp_dept_id == d.dept_id")
  joinDF.show(false)

  val joinDF2 = spark.sql("select * from EMP e INNER JOIN DEPT d ON e.emp_dept_id == d.dept_id")
  joinDF2.show(false)

}
