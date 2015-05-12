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

  test("To JSON") {
    val point = new GeometryValue((1.0, 1.0))
    assert(point.toJson == "{\"x\":1,\"y\":1,\"spatialReference\":{\"wkid\":4326}}")
    assert(point.toGeoJson == "{\"type\":\"Point\",\"coordinates\":[1.0,1.0]}")

    val line = new GeometryValue((1.0, 1.0), (2.0, 2.0))
    assert(line.toJson == "{\"paths\":[[[1,1],[2,2]]],\"spatialReference\":{\"wkid\":4326}}")
    assert(line.toGeoJson == "{\"type\":\"LineString\",\"coordinates\":[[1.0,1.0],[2.0,2.0]]}")
  }

  test("Factory methods") {
    val point = GeometryValue.point((1.0, 1.0))
    assert(point.toGeoJson == "{\"type\":\"Point\",\"coordinates\":[1.0,1.0]}")

    val line = GeometryValue.line((1.0, 1.0), (2.0, 2.0), (3.0, 3.0), (4.0, 4.0))
    assert(line.toGeoJson == "{\"type\":\"LineString\",\"coordinates\":[[1.0,1.0],[2.0,2.0],[3.0,3.0],[4.0,4.0]]}")

    val multiLine = GeometryValue.multiLine(Seq((1.0, 2.0), (2.0, 3.0)), Seq((10.0, 20.0), (20.0, 30.0)))
    assert(multiLine.toGeoJson == "{\"type\":\"MultiLineString\",\"coordinates\":[[[1.0,2.0],[2.0,3.0]],[[10.0,20.0],[20.0,30.0]]]}")

    val polygon = GeometryValue.polygon((1.0, 1.0), (2.0, 2.0), (3.0, 3.0))
    assert(polygon.toGeoJson == "{\"type\":\"Polygon\",\"coordinates\":[[1.0,1.0],[2.0,2.0],[3.0,3.0],[1.0,1.0]]}")

    val mPoly = GeometryValue.multiPolygon(
      Seq((1.0, 1.0), (2.0, 2.0), (3.0, 3.0)),
      Seq((0.1, 0.1), (0.2, 0.2), (0.3, 0.3)))
    assert(mPoly.toGeoJson == "{\"type\":\"MultiPolygon\",\"coordinates\":[[[1.0,1.0],[2.0,2.0],[3.0,3.0],[1.0,1.0]],[[0.1,0.1],[0.2,0.2],[0.3,0.3],[0.1,0.1]]]}")
  }

  test("From JSON") {
    val data = Seq(
      Row(1, GeometryValue.fromGeoJson(jsons(1))),
      Row(2, GeometryValue.fromGeoJson(jsons(2))),
      Row(3, GeometryValue.fromGeoJson(jsons(3)))
    )
    assert(data.mkString(",") ==
      "[1,{\"type\":\"Point\",\"coordinates\":[1.0,1.0]}]," +
        "[2,{\"type\":\"LineString\",\"coordinates\":[[12.0,13.0],[15.0,20.0]]}]," +
        "[3,{\"type\":\"MultiLineString\",\"coordinates\":[[[12.0,13.0],[15.0,20.0]],[[7.0,9.0],[11.0,17.0]]]}]");
  }

  test("Load JSON RDD with explicit schema") {
    val rdd = sparkContext.parallelize(Seq(
      "{\"id\":1,\"geo\":" + jsons(1) + "}",
      "{\"id\":2,\"geo\":" + jsons(2) + "}"
    ))
    val df = sqlContext.jsonRDD(rdd, schema)
    assert(df.collect().mkString(",") == "[1,{\"type\":\"Point\",\"coordinates\":[1.0,1.0]}],[2,{\"type\":\"LineString\",\"coordinates\":[[12.0,13.0],[15.0,20.0]]}]");
  }

  test("In RDDs") {
    val data = Seq(
      Row(1, GeometryValue.fromGeoJson(jsons(1))),
      Row(2, GeometryValue.fromGeoJson(jsons(2)))
    )
    val rdd = sparkContext.parallelize(data)
    val df = sqlContext.createDataFrame(rdd, schema)
    assert(df.collect().mkString(",") == "[1,{\"type\":\"Point\",\"coordinates\":[1.0,1.0]}],[2,{\"type\":\"LineString\",\"coordinates\":[[12.0,13.0],[15.0,20.0]]}]");

  }
}

