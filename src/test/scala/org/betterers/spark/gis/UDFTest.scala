package org.betterers.spark.gis

import org.betterers.spark.gis.udf.Functions
import org.scalatest.FunSuite

/**
 * UDF test suite
 */
class UDFTest extends FunSuite {
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
  val collection = Geometry.aggregate(point, multiPoint, line)
  val all: Seq[Geometry] = Seq(point, multiPoint, line, multiLine, polygon, multiPolygon, collection)

  test("ST_Boundary") {
    // all.foreach(g => println(Functions.ST_Boundary(g).toString))

    assertResult("Some(POINT EMPTY)") {
      Functions.ST_Boundary(point).toString
    }
    assertResult("Some(POINT EMPTY)") {
      Functions.ST_Boundary(multiPoint).toString
    }
    assertResult("Some(MULTIPOINT ((11 11), (12 12)))") {
      Functions.ST_Boundary(line).toString
    }
    assertResult("Some(MULTIPOINT ((11 1), (23 23), (31 3), (42 42)))") {
      Functions.ST_Boundary(multiLine).toString
    }
    assertResult("Some(LINESTRING (1 1, 3 1, 2 2, 1 1))") {
      Functions.ST_Boundary(polygon).toString
    }
    assertResult("Some(MULTILINESTRING ((1 1, 3 1, 2 2, 1 1), (1.1 1.1, 2.5 1.1, 2 1.9, 1.1 1.1)))") {
      Functions.ST_Boundary(multiPolygon).toString
    }
    assertResult(None) {
      Functions.ST_Boundary(collection)
    }
  }

  test("ST_CoordDim") {
    all.foreach(g => {
      assertResult(2) {
        Functions.ST_CoordDim(g)
      }
    })
  }
}
