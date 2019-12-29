package com.sparkbyexamples.spark.dataframe.join

import org.apache.spark.sql.SparkSession

object JoinMultipleDataFrames extends App {

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val emp = Seq((1,"Smith","10"),
    (2,"Rose","20"),
    (3,"Williams","10"),
    (4,"Jones","10"),
    (5,"Brown","40"),
    (6,"Brown","50")
  )
  val empColumns = Seq("emp_id","name","emp_dept_id")
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

  val address = Seq((1,"1523 Main St","SFO","CA"),
    (2,"3453 Orange St","SFO","NY"),
    (3,"34 Warner St","Jersey","NJ"),
    (4,"221 Cavalier St","Newark","DE"),
    (5,"789 Walnut St","Sandiago","CA")
  )
  val addColumns = Seq("emp_id","addline1","city","state")
  val addDF = address.toDF(addColumns:_*)
  addDF.show(false)

  //Using Join expression
  empDF.join(deptDF,empDF("emp_dept_id") === deptDF("dept_id"),"inner" )
      .join(addDF,empDF("emp_id") === addDF("emp_id"),"inner")
      .show(false)

  //Using where
  empDF.join(deptDF).where(empDF("emp_dept_id") === deptDF("dept_id"))
    .join(addDF).where(empDF("emp_id") === addDF("emp_id"))
    .show(false)

  //Using Filter
  empDF.join(deptDF).filter(empDF("emp_dept_id") === deptDF("dept_id"))
    .join(addDF).filter(empDF("emp_id") === addDF("emp_id"))
    .show(false)

  //Using SQL expression
  empDF.createOrReplaceTempView("EMP")
  deptDF.createOrReplaceTempView("DEPT")
  addDF.createOrReplaceTempView("ADD")

  spark.sql("select * from EMP e, DEPT d, ADD a " +
    "where e.emp_dept_id == d.dept_id and e.emp_id == a.emp_id")
    .show(false)


}
