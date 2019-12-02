package com.sparkbyexamples.spark.rdd

import com.sparkbyexamples.spark.rdd.OperationOnPairRDDComplex.kv
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object RDDActions extends App {

  val spark = SparkSession.builder()
    .appName("SparkByExample")
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  val inputRDD = spark.sparkContext.parallelize(List(("Z", 1),("A", 20),("B", 30),("C", 40),("B", 30),("B", 60)))

  val listRdd = spark.sparkContext.parallelize(List(1,2,3,4,5,3,2))

  //Collect
  val data:Array[Int] = listRdd.collect()
  data.foreach(println)

  //aggregate
  def param0= (accu:Int, v:Int) => accu + v
  def param1= (accu1:Int,accu2:Int) => accu1 + accu2
  println("aggregate : "+listRdd.aggregate(0)(param0,param1))
  //Output: aggregate : 20

  //aggregate
  def param3= (accu:Int, v:(String,Int)) => accu + v._2
  def param4= (accu1:Int,accu2:Int) => accu1 + accu2
  println("aggregate : "+inputRDD.aggregate(0)(param3,param4))
  //Output: aggregate : 20

  //treeAggregate. This is similar to aggregate
  def param8= (accu:Int, v:Int) => accu + v
  def param9= (accu1:Int,accu2:Int) => accu1 + accu2
  println("treeAggregate : "+listRdd.treeAggregate(0)(param8,param9))
  //Output: treeAggregate : 20

  //fold
  println("fold :  "+listRdd.fold(0){ (acc,v) =>
    val sum = acc+v
    sum
  })
  //Output: fold :  20

  println("fold :  "+inputRDD.fold(("Total",0)){(acc:(String,Int),v:(String,Int))=>
    val sum = acc._2 + v._2
    ("Total",sum)
  })
  //Output: fold :  (Total,181)

  //reduce
  println("reduce : "+listRdd.reduce(_ + _))
  //Output: reduce : 20
  println("reduce alternate : "+listRdd.reduce((x, y) => x + y))
  //Output: reduce alternate : 20
  println("reduce : "+inputRDD.reduce((x, y) => ("Total",x._2 + y._2)))
  //Output: reduce : (Total,181)

  //treeReduce. This is similar to reduce
  println("treeReduce : "+listRdd.treeReduce(_ + _))
  //Output: treeReduce : 20

  //count, countApprox, countApproxDistinct
  println("Count : "+listRdd.count)
  //Output: Count : 20
  println("countApprox : "+listRdd.countApprox(1200))
  //Output: countApprox : (final: [7.000, 7.000])
  println("countApproxDistinct : "+listRdd.countApproxDistinct())
  //Output: countApproxDistinct : 5
  println("countApproxDistinct : "+inputRDD.countApproxDistinct())
  //Output: countApproxDistinct : 5

  //countByValue, countByValueApprox
  println("countByValue :  "+listRdd.countByValue())
  //Output: countByValue :  Map(5 -> 1, 1 -> 1, 2 -> 2, 3 -> 2, 4 -> 1)
  //println(listRdd.countByValueApprox())

  //first
  println("first :  "+listRdd.first())
  //Output: first :  1
  println("first :  "+inputRDD.first())
  //Output: first :  (Z,1)

  //top
  println("top : "+listRdd.top(2).mkString(","))
  //Output: take : 5,4
  println("top : "+inputRDD.top(2).mkString(","))
  //Output: take : (Z,1),(C,40)

  //min
  println("min :  "+listRdd.min())
  //Output: min :  1
  println("min :  "+inputRDD.min())
  //Output: min :  (A,20)

  //max
  println("max :  "+listRdd.max())
  //Output: max :  5
  println("max :  "+inputRDD.max())
  //Output: max :  (Z,1)

  //take, takeOrdered, takeSample
  println("take : "+listRdd.take(2).mkString(","))
  //Output: take : 1,2
  println("takeOrdered : "+ listRdd.takeOrdered(2).mkString(","))
  //Output: takeOrdered : 1,2
  //println("take : "+listRdd.takeSample())

  //toLocalIterator
  //listRdd.toLocalIterator.foreach(println)
  //Output:

}
