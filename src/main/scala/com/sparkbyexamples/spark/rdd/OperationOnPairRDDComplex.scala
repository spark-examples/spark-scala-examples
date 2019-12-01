package com.sparkbyexamples.spark.rdd

import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object OperationOnPairRDDComplex extends App{


  val spark = SparkSession.builder()
    .appName("SparkByExample")
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val keysWithValuesList = Array("foo=A", "foo=A", "foo=A", "foo=A", "foo=B", "bar=C", "bar=D", "bar=D")
  val data = spark.sparkContext.parallelize(keysWithValuesList)
  //Create key value pairs
  val kv = data.map(_.split("=")).map(v => (v(0), v(1))).cache()
  kv.foreach(println)

  val se = mutable.HashSet.empty[String]
  def param3= (accu:mutable.HashSet[String], v:String) => accu + v
  def param4= (accu1:mutable.HashSet[String],accu2:mutable.HashSet[String]) => accu1 ++= accu2
  kv.aggregateByKey(se)(param3,param4).foreach(println)
  //    Aggregate By Key unique Results
  //    bar -> C,D
  //    foo -> B,A

  def param5= (accu:Int, v:String) => accu + 1
  def param6= (accu1:Int,accu2:Int) => accu1 + accu2
  kv.aggregateByKey(0)(param5,param6).foreach(println)
  //    Aggregate By Key sum Results
  //    bar -> 3
  //    foo -> 5

  val studentRDD = spark.sparkContext.parallelize(Array(
    ("Joseph", "Maths", 83), ("Joseph", "Physics", 74), ("Joseph", "Chemistry", 91), ("Joseph", "Biology", 82),
    ("Jimmy", "Maths", 69), ("Jimmy", "Physics", 62), ("Jimmy", "Chemistry", 97), ("Jimmy", "Biology", 80),
    ("Tina", "Maths", 78), ("Tina", "Physics", 73), ("Tina", "Chemistry", 68), ("Tina", "Biology", 87),
    ("Thomas", "Maths", 87), ("Thomas", "Physics", 93), ("Thomas", "Chemistry", 91), ("Thomas", "Biology", 74),
    ("Cory", "Maths", 56), ("Cory", "Physics", 65), ("Cory", "Chemistry", 71), ("Cory", "Biology", 68),
    ("Jackeline", "Maths", 86), ("Jackeline", "Physics", 62), ("Jackeline", "Chemistry", 75), ("Jackeline", "Biology", 83),
    ("Juan", "Maths", 63), ("Juan", "Physics", 69), ("Juan", "Chemistry", 64), ("Juan", "Biology", 60)), 3)

  //ReducebyKey returns same datatype
  val studentKey = studentRDD.map(f=> (f._1,(f._2,f._3)))
  studentKey.reduceByKey((accu,v)=>{
    (accu._1,if(accu._2 > v._2)accu._2 else v._2)
  }).foreach(println)

  //Similar example with aggregateByKey
  val def1 = (accu:Int, v:(String,Int)) => if(accu > v._2) accu else v._2
  val def2 = (accu1:Int,accu2:Int) => if( accu1 > accu2) accu1 else accu2
  studentKey.aggregateByKey(0)(def1,def2).foreach(println)
  // Output
  // (Tina,87)
  // (Thomas,93)
  // (Jackeline,83)
  // (Joseph,91)
  // (Juan,69)
  // (Jimmy,97)
  // (Cory,71)

  val zeroval = ("",0)
  val subjectKey = studentRDD.map(f=> (f._2,(f._1,f._3)))
  val def3 = (accu:(String,Int), v:(String,Int)) => if(accu._2 > v._2) accu else v
  val def4 = (accu1:(String,Int),accu2:(String,Int)) => if (accu1._2 > accu2._2) accu1 else accu2
  subjectKey.aggregateByKey(zeroval)(def3,def4).foreach(println)
  //    (Chemistry,(Jimmy,97))
  //    (Biology,(Tina,87))
  //    (Maths,(Thomas,87))
  //    (Physics,(Thomas,93))

  val def5 = (accu:Int, v:(String,Int)) => accu + v._2
  val def6 = (accu1:Int,accu2:Int) => accu1 + accu2
  val studentTotals = studentKey.aggregateByKey(0)(def5,def6)
  studentTotals.foreach(println)
  //    (Tina,306)
  //    (Thomas,345)
  //    (Jackeline,306)
  //    (Joseph,330)
  //    (Juan,256)
  //    (Jimmy,308)
  //    (Cory,260)

  //Student with highest score
  val tot = studentTotals.max()(new Ordering[Tuple2[String, Int]]() {
    override def compare(x: (String, Int), y: (String, Int)): Int =
      Ordering[Int].compare(x._2, y._2)
  })
  println("First class student : "+tot._1 +"=" + tot._2)


  //combinekeykey
  //foldbykey
}
