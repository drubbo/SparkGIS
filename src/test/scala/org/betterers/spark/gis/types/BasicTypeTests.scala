package org.betterers.spark.gis.types

import org.apache.spark.sql.test.TestSQLContext._
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.scalatest.FunSuite

/**
 * Basic tests for GIS UDTs
 */
class BasicTypeTests extends FunSuite {
  val sqlContext = new SQLContext(sparkContext)
  val schema = StructType(Seq(
    StructField("id", IntegerType),
    StructField("geo", GeometryType.T)
  ))
  val jsons = Map(
    (1,"{\"type\":\"Point\",\"coordinates\":[1,1]}}"),
    (2,"{\"type\":\"LineString\",\"coordinates\":[[12,13],[15,20]]}}")
  )

  test("Serialize") {
    val point = new GeometryType((1.0, 1.0))
    println(point.toJson)
    println(point.toGeoJson)
    val line = new GeometryType((1.0, 1.0), (2.0, 2.0))
    println(line.toJson)
    println(line.toGeoJson)
    val poly = new GeometryType((1.0, 1.0), (2.0, 2.0), (3.0, 3.0), (4.0, 4.0))
    println(poly.toJson)
    println(poly.toGeoJson)
  }

  test("Deserialize") {
    val data = Seq(
      Row(1, GeometryType.fromGeoJson(jsons(1))),
      Row(2, GeometryType.fromGeoJson(jsons(2)))
    )
    println(data)
  }

  test("In RDDs") {
    val data = Seq(
      Row(1, GeometryType.fromGeoJson(jsons(1))),
      Row(2, GeometryType.fromGeoJson(jsons(2)))
    )
    val rdd = sparkContext.parallelize(data)
    val df = sqlContext.createDataFrame(rdd, schema)
    df.foreach(println)
  }

  test("Load JSON RDD with explicit schema") {
    val rdd = sparkContext.parallelize(Seq(
      "{\"id\":1,\"geo\":" + jsons(1) + "}",
      "{\"id\":2,\"geo\":" + jsons(2) + "}"
    ))
    val df = sqlContext.jsonRDD(rdd, schema)
    df.foreach(println)
  }
}

