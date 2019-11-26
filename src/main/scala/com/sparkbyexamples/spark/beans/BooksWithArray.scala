package com.sparkbyexamples.spark.beans

case class BooksWithArray(_id:String, author:String, description:String, price:Double, publish_date:String, title:String,otherInfo:OtherInfo,stores:Stores)
case class OtherInfo(pagesCount:String,language:String,country:String,address:Address)
case class Address(addressline1:String,city:String,state:String)
case class Stores(store:Array[Store])
case class Store(name:String)


