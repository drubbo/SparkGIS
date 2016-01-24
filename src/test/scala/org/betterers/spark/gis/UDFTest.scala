package org.betterers.spark.gis

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{SQLContext, Row}
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.betterers.spark.gis.udf.Functions

/**
 * UDF test suite
 */
class UDFTest extends FunSuite with BeforeAndAfter {
  import Geometry.WGS84

  val point = Geometry.point((2.0, 2.0))
  val multiPoint = Geometry.multiPoint((1.0, 1.0), (2.0, 2.0), (3.0, 3.0))
  var line = Geometry.line((11.0, 11.0), (12.0, 12.0))
  var multiLine = Geometry.multiLine(
    Seq((11.0, 1.0), (23.0, 23.0)),
    Seq((31.0, 3.0), (42.0, 42.0)))
  var polygon = Geometry.polygon((1.0, 1.0), (2.0, 2.0), (3.0, 1.0))
  var multiPolygon = Geometry.multiPolygon(
    Seq((1.0, 1.0), (2.0, 2.0), (3.0, 1.0)),
    Seq((1.1, 1.1), (2.0, 1.9), (2.5, 1.1))
  )
  val collection = Geometry.collection(point, multiPoint, line)
  val all: Seq[Geometry] = Seq(point, multiPoint, line, multiLine, polygon, multiPolygon, collection)

  var sc: SparkContext = _
  var sql: SQLContext = _

  before {
    sc = new SparkContext(new SparkConf().setMaster("local[4]").setAppName("SparkGIS"))
    sql = new SQLContext(sc)
  }

  after {
    sc.stop()
  }

  test("ST_Boundary") {
    // all.foreach(g => println(Functions.ST_Boundary(g).toString))

    assertResult(true) {
      Functions.ST_Boundary(point).isEmpty
    }
    assertResult(true) {
      Functions.ST_Boundary(multiPoint).isEmpty
    }
    assertResult("Some(MULTIPOINT ((11 11), (12 12)))") {
      Functions.ST_Boundary(line).toString
    }
    assertResult(None) {
      Functions.ST_Boundary(multiLine)
    }
    assertResult("Some(LINEARRING (1 1, 2 2, 3 1, 1 1))") {
      Functions.ST_Boundary(polygon).toString
    }
    assertResult(None) {
      Functions.ST_Boundary(multiPolygon)
    }
    assertResult(None) {
      Functions.ST_Boundary(collection)
    }
  }

  test("ST_CoordDim") {
    all.foreach(g => {
      assertResult(3) {
        Functions.ST_CoordDim(g)
      }
    })
  }

  test("UDF in SQL") {
    val schema = StructType(Seq(
      StructField("id", IntegerType),
      StructField("geo", GeometryType.Instance)
    ))
    val jsons = Map(
      (1, "{\"type\":\"Point\",\"coordinates\":[1,1]}}"),
      (2, "{\"type\":\"LineString\",\"coordinates\":[[12,13],[15,20]]}}")
    )
    val rdd = sc.parallelize(Seq(
      "{\"id\":1,\"geo\":" + jsons(1) + "}",
      "{\"id\":2,\"geo\":" + jsons(2) + "}"
    ))
    rdd.name = "TEST"
    val df = sql.read.schema(schema).json(rdd)
    df.registerTempTable("TEST")
    Functions.register(sql)
    assertResult(Array(3,3)) {
      sql.sql("SELECT ST_CoordDim(geo) FROM TEST").collect().map(_.get(0))
    }
  }
}
