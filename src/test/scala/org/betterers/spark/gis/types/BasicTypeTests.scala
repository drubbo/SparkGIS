package scala.org.betterers.spark.gis.types

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.test.TestSQLContext._
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite

import org.betteres.spark.gis.types.Geometry

/**
 * Basic tests for GIS UDTs
 */
class BasicTypeTests extends FunSuite{
  val sqlContext = new SQLContext(sparkContext)

  test("How are they serialized") {
    val schema = StructType(Seq(StructField("id", IntegerType), StructField("geo", Geometry.Type)))
    val rdd = sparkContext.parallelize(Seq(
      Row(1, "{\"type\":\"Point\",\"coordinates\":[1,1]}}"),
      Row(2, "{\"type\":\"Point\",\"coordinates\":[12,13]}}")
    ))
    val schemaRDD = sqlContext.createDataFrame(rdd, schema)
    schemaRDD.foreach(println)
  }

  test("Point types from GeoJSON") {
  }
}

