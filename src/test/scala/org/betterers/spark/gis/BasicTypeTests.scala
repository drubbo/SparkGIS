package org.betterers.spark.gis

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{SQLContext, Row}
import org.scalatest.{BeforeAndAfter, FunSuite}

/**
 * Basic tests for GIS UDTs
 */
class BasicTypeTests extends FunSuite with BeforeAndAfter {

  import Geometry.WGS84

  val schema = StructType(Seq(
    StructField("id", IntegerType),
    StructField("geo", GeometryType.Instance)
  ))
  val jsons = Map(
    (1, "{\"type\":\"Point\",\"coordinates\":[1,1]}"),
    (2, "{\"type\":\"LineString\",\"coordinates\":[[12,13],[15,20]]}"),
    (3, "{\"type\":\"MultiLineString\",\"coordinates\":[[[12,13],[15,20]],[[7,9],[11,17]]]}")
  )

  var sc: SparkContext = _
  var sql: SQLContext = _

  before {
    sc = new SparkContext(new SparkConf().setMaster("local[4]").setAppName("SparkGIS"))
    sql = new SQLContext(sc)
  }

  after {
    sc.stop()
  }

  test("Factory methods") {
    val point = Geometry.point((1.0, 1.0))
    assertResult("POINT (1 1)") {
      point.toString
    }

    val multiPoint = Geometry.multiPoint((1.0, 1.0), (2.0, 2.0))
    assertResult("MULTIPOINT ((1 1), (2 2))") {
      multiPoint.toString
    }

    val line = Geometry.line((1.0, 1.0), (2.0, 2.0), (3.0, 3.0), (4.0, 4.0))
    assertResult("LINESTRING (1 1, 2 2, 3 3, 4 4)") {
      line.toString
    }

    val multiLine = Geometry.multiLine(Seq((1.0, 2.0), (2.0, 3.0)), Seq((10.0, 20.0), (20.0, 30.0)))
    assertResult("MULTILINESTRING ((1 2, 2 3), (10 20, 20 30))") {
      multiLine.toString
    }

    val polygon = Geometry.polygon((1.0, 1.0), (2.0, 2.0), (3.0, 1.0), (2.0, 0.0))
    assertResult("POLYGON ((1 1, 2 2, 3 1, 2 0, 1 1))") {
      polygon.toString
    }

    val mPoly = Geometry.multiPolygon(
      Seq((1.0, 1.0), (1.0, 2.0), (2.0, 2.0), (2.0, 1.0)),
      Seq((1.25, 1.25), (1.25, 1.75), (1.75, 1.75), (1.75, 1.25))
    )
    assertResult("MULTIPOLYGON (((1 1, 1 2, 2 2, 2 1, 1 1)), ((1.25 1.25, 1.25 1.75, 1.75 1.75, 1.75 1.25, 1.25 1.25)))") {
      mPoly.toString
    }

    val coll = Geometry.collection(point, line, polygon)
    assertResult("GEOMETRYCOLLECTION (POINT (1 1), LINESTRING (1 1, 2 2, 3 3, 4 4), POLYGON ((1 1, 2 2, 3 1, 2 0, 1 1)))") {
      coll.toString
    }
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
    import Geometry.ImplicitConversions._

    val point = Geometry.point((1.0, 1.0))
    assertResult("{\"type\":\"Point\",\"coordinates\":[1,1]}") {
      point.toGeoJson
    }

    val line = Geometry.line((1.0, 1.0), (2.0, 2.0))
    assertResult("{\"type\":\"LineString\",\"coordinates\":[[1,1],[2,2]]}") {
      line.toGeoJson
    }

    val polygon = Geometry.polygon((1.0, 1.0), (2.0, 2.0), (3.0, 1.0), (2.0, 0.0))
    assertResult("{\"type\":\"Polygon\",\"coordinates\":[[[1,1],[2,2],[3,1],[2,0.0],[1,1]]]}") {
      polygon.toGeoJson
    }

    val mPoly = Geometry.multiPolygon(
      Seq((1.0, 1.0), (1.0, 2.0), (2.0, 2.0), (2.0, 1.0)),
      Seq((1.25, 1.25), (1.25, 1.75), (1.75, 1.75), (1.75, 1.25))
    )
    assertResult("{\"type\":\"MultiPolygon\",\"coordinates\":[[[[1,1],[1,2],[2,2],[2,1],[1,1]]],[[[1.25,1.25],[1.25,1.75],[1.75,1.75],[1.75,1.25],[1.25,1.25]]]]}") {
      mPoly.toGeoJson
    }

    val coll = Geometry.collection(point, line, polygon)
    assertResult("{\"type\":\"GeometryCollection\",\"geometries\":[{\"type\":\"Point\",\"coordinates\":[1,1]},{\"type\":\"LineString\",\"coordinates\":[[1,1],[2,2]]},{\"type\":\"Polygon\",\"coordinates\":[[[1,1],[2,2],[3,1],[2,0.0],[1,1]]]}]}") {
      coll.toGeoJson
    }
  }

  test("Deserialize") {
    val p1 = GeometryType.Instance.deserialize("POINT (1 1)")
    val p2 = GeometryType.Instance.deserialize("{\"type\":\"Point\",\"coordinates\":[1.0,1.0]}")
    assert(p1 == p2)
  }

  test("Load JSON RDD with explicit schema") {
    val rdd = sc.parallelize(Seq(
      "{\"id\":1,\"geo\":" + jsons(1) + "}",
      "{\"id\":2,\"geo\":" + jsons(2) + "}"
    ))
    val df = sql.read.schema(schema).json(rdd)
    assert(df.collect().mkString(",") == "[1,POINT (1 1)],[2,LINESTRING (12 13, 15 20)]")
  }

  test("Values in RDDs") {
    val data = Seq(
      Row(1, Geometry.fromGeoJson(jsons(1))),
      Row(2, Geometry.fromGeoJson(jsons(2)))
    )
    val rdd = sc.parallelize(data)
    val df = sql.createDataFrame(rdd, schema)
    assert(df.collect().mkString(",") == "[1,POINT (1 1)],[2,LINESTRING (12 13, 15 20)]")
  }
}

