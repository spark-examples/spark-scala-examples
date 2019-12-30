package com.sparkbyexamples.spark.dataframe
import org.apache.spark.sql.types.{IntegerType, _}
object DataTypeExample extends App{




  val typeFromJson = DataType.fromJson(
    """{"type":"array",
      |"elementType":"string","containsNull":false}""".stripMargin)
  println(typeFromJson.getClass)
  val typeFromJson2 = DataType.fromJson("\"string\"")
  println(typeFromJson2.getClass)

  val ddlSchemaStr = "`fullName` STRUCT<`first`: STRING, `last`: STRING," +
    "`middle`: STRING>,`age` INT,`gender` STRING"
  val ddlSchema = DataType.fromDDL(ddlSchemaStr)
  println(ddlSchema.getClass)
  //DataType.canWrite()
  //DataType.equalsStructurally()

  //StringType
  val stringType = DataTypes.StringType
  println("json : "+stringType.json)  // Represents json string of datatype
  println("prettyJson : "+stringType.prettyJson) // Gets json in pretty format
  println("simpleString : "+stringType.simpleString)
  println("sql : "+stringType.sql)
  println("typeName : "+stringType.typeName)
  println("catalogString : "+stringType.catalogString)
  println("defaultSize : "+stringType.defaultSize)



  //ArrayType
  val arr = ArrayType(IntegerType,false)
  val arrayType = DataTypes.createArrayType(StringType,true)
  println("json() : "+arrayType.json)  // Represents json string of datatype
  println("prettyJson() : "+arrayType.prettyJson) // Gets json in pretty format
  println("simpleString() : "+arrayType.simpleString) // simple string
  println("sql() : "+arrayType.sql) // SQL format
  println("typeName() : "+arrayType.typeName) // type name
  println("catalogString() : "+arrayType.catalogString) // catalog string
  println("defaultSize() : "+arrayType.defaultSize) // default size

  println("containsNull : "+arrayType.containsNull)
  println("elementType : "+arrayType.elementType)
  println("productElement : "+arrayType.productElement(0))

  //MapType
  val mapType1 = MapType(StringType,IntegerType)
  val mapType = DataTypes.createMapType(StringType,IntegerType)
  println("keyType() : "+mapType.keyType)
  println("valueType : "+mapType.valueType)
  println("valueContainsNull : "+mapType.valueContainsNull)
  println("productElement : "+mapType.productElement(1))

  //TimestampType
  val timestampType1 = TimestampType
  val timestampType = DataTypes.TimestampType

  //StructType
  val structType = DataTypes.createStructType(
    Array(DataTypes.createStructField("fieldName",StringType,true)))

  val simpleSchema = StructType(Array(
    StructField("name",StringType,true),
    StructField("id", IntegerType, true),
    StructField("gender", StringType, true),
    StructField("salary", DoubleType, true)
  ))

  val anotherSchema = new StructType()
    .add("name",new StructType()
      .add("firstname",StringType)
      .add("lastname",StringType))
    .add("id",IntegerType)
    .add("salary",DoubleType)


}
