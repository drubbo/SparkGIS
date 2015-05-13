package org.betterers.spark.gis

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
    StructField("geo", GeometryType.Instance)
  ))
  val jsons = Map(
    (1, "{\"type\":\"Point\",\"coordinates\":[1,1]}}"),
    (2, "{\"type\":\"LineString\",\"coordinates\":[[12,13],[15,20]]}}"),
    (3, "{\"type\":\"MultiLineString\",\"coordinates\":[[[12,13],[15,20]],[[7,9],[11,17]]]}}")
  )

  test("Factory methods") {
    val point = Geometry.point((1.0, 1.0))
    assert(point.toString == "POINT (1 1)")

    val multiPoint = Geometry.multiPoint((1.0, 1.0), (2.0, 2.0))
    assert(multiPoint.toString == "MULTIPOINT ((1 1), (2 2))")

    val line = Geometry.line((1.0, 1.0), (2.0, 2.0), (3.0, 3.0), (4.0, 4.0))
    assert(line.toString == "LINESTRING (1 1, 2 2, 3 3, 4 4)")

    val multiLine = Geometry.multiLine(Seq((1.0, 2.0), (2.0, 3.0)), Seq((10.0, 20.0), (20.0, 30.0)))
    assert(multiLine.toString == "MULTILINESTRING ((1 2, 2 3), (10 20, 20 30))")

    val polygon = Geometry.polygon((1.0, 1.0), (2.0, 2.0), (3.0, 1.0), (2.0, 0.0))
    assert(polygon.toString == "POLYGON ((1 1, 2 0, 3 1, 2 2, 1 1))")

    val mPoly = Geometry.multiPolygon(
      Seq((1.0, 1.0), (1.0, 2.0), (2.0, 2.0), (2.0, 1.0)),
      Seq((1.25, 1.25), (1.25, 1.75), (1.75, 1.75), (1.75, 1.25))
    )
    assert(mPoly.toString == "MULTIPOLYGON (((1 1, 2 1, 2 2, 1 2, 1 1)), ((1.25 1.25, 1.75 1.25, 1.75 1.75, 1.25 1.75, 1.25 1.25)))")

    val coll = Geometry.aggregate(point, line, polygon)
    assert(coll.toString == "GEOMETRYCOLLECTION (POINT (1 1), LINESTRING (1 1, 2 2, 3 3, 4 4), POLYGON ((1 1, 2 0, 3 1, 2 2, 1 1)))")
  }

  test("From JSON") {
    val data = Seq(
      Row(1, Geometry.fromGeoJson(jsons(1))),
      Row(2, Geometry.fromGeoJson(jsons(2))),
      Row(3, Geometry.fromGeoJson(jsons(3)))
    )
    assert(data.mkString(",") ==
      "[1,POINT (1 1)]," +
      "[2,LINESTRING (12 13, 15 20)]," +
      "[3,MULTILINESTRING ((12 13, 15 20), (7 9, 11 17))]")
  }

  test("To JSON") {
    val point = Geometry.point((1.0, 1.0))
    assert(point.toJson == "{\"x\":1,\"y\":1,\"spatialReference\":{\"wkid\":4326}}")
    assert(point.toGeoJson == "{\"type\":\"Point\",\"coordinates\":[1.0,1.0]}")

    val line = Geometry.line((1.0, 1.0), (2.0, 2.0))
    assert(line.toJson == "{\"paths\":[[[1,1],[2,2]]],\"spatialReference\":{\"wkid\":4326}}")
    assert(line.toGeoJson == "{\"type\":\"LineString\",\"coordinates\":[[1.0,1.0],[2.0,2.0]]}")

    val polygon = Geometry.polygon((1.0, 1.0), (2.0, 2.0), (3.0, 1.0), (2.0, 0.0))
    assert(polygon.toJson == "{\"rings\":[[[1,1],[2,2],[3,1],[2,0],[1,1]]],\"spatialReference\":{\"wkid\":4326}}")
    assert(polygon.toGeoJson == "{\"type\":\"Polygon\",\"coordinates\":[[[1.0,1.0],[2.0,2.0],[3.0,1.0],[2.0,0.0],[1.0,1.0]]]}")
  }

  test("Deserialize") {
    val p1 = GeometryType.Instance.deserialize("POINT (1 1)")
    val p2 = GeometryType.Instance.deserialize("{\"x\":1,\"y\":1,\"spatialReference\":{\"wkid\":4326}}")
    val p3 = GeometryType.Instance.deserialize("{\"type\":\"Point\",\"coordinates\":[1.0,1.0]}")
    assert(p1 == p2)
    assert(p1 == p3)
    assert(p2 == p3)
  }

  test("Load JSON RDD with explicit schema") {
    val rdd = sparkContext.parallelize(Seq(
      "{\"id\":1,\"geo\":" + jsons(1) + "}",
      "{\"id\":2,\"geo\":" + jsons(2) + "}"
    ))
    val df = sqlContext.jsonRDD(rdd, schema)
    assert(df.collect().mkString(",") == "[1,POINT (1 1)],[2,LINESTRING (12 13, 15 20)]")
  }

  test("Values in RDDs") {
    val data = Seq(
      Row(1, Geometry.fromGeoJson(jsons(1))),
      Row(2, Geometry.fromGeoJson(jsons(2)))
    )
    val rdd = sparkContext.parallelize(data)
    val df = sqlContext.createDataFrame(rdd, schema)
    assert(df.collect().mkString(",") == "[1,POINT (1 1)],[2,LINESTRING (12 13, 15 20)]")

  }
}

