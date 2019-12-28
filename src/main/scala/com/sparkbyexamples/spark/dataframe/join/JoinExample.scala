package com.sparkbyexamples.spark.dataframe.join

import org.apache.spark.sql.SparkSession

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
      (6,"Brown",2,"2010","","",-1)
  )
  val empColumns = Seq("emp_id","name","superior_emp_id","year_joined","dept_id","gender","salary")
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

  println("Using crossJoin()")
  empDF.crossJoin(deptDF).show(false)

  // Join
  empDF.join(deptDF,empDF("dept_id") ===  deptDF("dept_id"))
    .show(false)
  empDF.join(deptDF,"dept_id")
    .show(false)
  println("cross join")
  empDF.join(deptDF,empDF("dept_id") ===  deptDF("dept_id"),"cross")
    .show(false)
  println("Inner join")
  empDF.join(deptDF,empDF("dept_id") ===  deptDF("dept_id"),"inner")
    .show(false)

  println("Outer join")
  empDF.join(deptDF,empDF("dept_id") ===  deptDF("dept_id"),"outer")
    .show(false)
  println("full join")
  empDF.join(deptDF,empDF("dept_id") ===  deptDF("dept_id"),"full")
    .show(false)
  println("fullouter join")
  empDF.join(deptDF,empDF("dept_id") ===  deptDF("dept_id"),"fullouter")
    .show(false)

  println("left join")
  empDF.join(deptDF,empDF("dept_id") ===  deptDF("dept_id"),"left")
    .show(false)
  println("leftouter join")
  empDF.join(deptDF,empDF("dept_id") ===  deptDF("dept_id"),"leftouter")
    .show(false)

  println("leftanti join")
  empDF.join(deptDF,empDF("dept_id") ===  deptDF("dept_id"),"leftanti")
    .show(false)

  println("leftsemi join")
  empDF.join(deptDF,empDF("dept_id") ===  deptDF("dept_id"),"leftsemi")
    .show(false)

}
