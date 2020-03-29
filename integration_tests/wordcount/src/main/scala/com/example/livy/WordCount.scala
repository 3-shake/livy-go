package com.example.livy

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types._

object WordCount {
  lazy val session: SparkSession =
    SparkSession.builder().appName("wordcount").config("spark.sql.shuffle.partitions", "1").getOrCreate()


  def main(args: Array[String]): Unit = {
    val ds = executor()
    ds.show
  }

  def executor(): Dataset[Row] = {
    val ds = testSinkDataset()

    ds
  }

  private[this] def testSinkDataset(): Dataset[Row] = {
      val arrayData = Seq(
        Row("James", List("Java","Scala"),
          Map("hair" -> "black", "eye" -> "brown"),
          List(Map("c" -> 1)),
          Row("string", Map("a" -> 1, "b" -> 2), List("a", "b", "c")),
          List(Row("key", "value"), Row("key", "value"))),
        Row("Michael", List("Spark","Java",null),
          Map("hair" -> "brown", "eye" -> null),
          List(Map("java" -> 3)),
          Row("string", Map("a" -> 1, "b" -> 2), List("a", "b", "c")),
          List(Row("key", "value"), Row("key", "value"))),
        Row("Robert", List("CSharp",""),
          Map("hair" -> "red","eye" -> ""),
          List(Map("ruby" -> 5)),
          Row("string", Map("a" -> 1, "b" -> 2), List("a", "b", "c")),
          List(Row("key", "value"), Row("key", "value"))),
        Row("Washington", null, null, null, null, null),
        Row("Jefferson", List(), Map(), List(), null, List())
      )

      val arraySchema = new StructType()
        .add("stringType",StringType)
        .add("stringInArrayType", ArrayType(StringType))
        .add("mapType", MapType(StringType,StringType))
        .add("mapTypeInArrayType", ArrayType(MapType(StringType, IntegerType)))
        .add("structType", new StructType()
          .add("stringType", StringType)
          .add("mapType", MapType(StringType, IntegerType))
          .add("arrayType", ArrayType(StringType)))
        .add("structTypeInArrayType", ArrayType(new StructType()
          .add("key", StringType)
          .add("value", StringType)))

      session.createDataFrame(session.sparkContext.parallelize(arrayData), arraySchema)
  }
}
