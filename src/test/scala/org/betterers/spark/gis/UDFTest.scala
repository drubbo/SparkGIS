package org.betterers.spark.gis

import org.betterers.spark.gis.udf.Functions
import org.scalatest.FunSuite

/**
 * UDF test suite
 *
 * @author drubbo <ubik@gamezoo.it>
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
    //all.foreach(g => println(Functions.ST_Boundary(g).map(_.toGeoJson)))
    assert(Functions.ST_Boundary(point).map(_.toGeoJson) == Some("{\"type\":\"Point\",\"coordinates\":null}"))
    assert(Functions.ST_Boundary(multiPoint).map(_.toGeoJson) == Some("{\"type\":\"Point\",\"coordinates\":null}"))
    assert(Functions.ST_Boundary(line).map(_.toGeoJson) == Some("{\"type\":\"MultiPoint\",\"coordinates\":[[11.0,11.0],[12.0,12.0]]}"))
    assert(Functions.ST_Boundary(multiLine).map(_.toGeoJson) == Some("{\"type\":\"MultiPoint\",\"coordinates\":[[11.0,1.0],[23.0,23.0],[31.0,3.0],[42.0,42.0]]}"))
    assert(Functions.ST_Boundary(polygon).map(_.toGeoJson) == Some("{\"type\":\"LineString\",\"coordinates\":[[1.0,1.0],[3.0,1.0],[2.0,2.0],[1.0,1.0]]}"))
    assert(Functions.ST_Boundary(multiPolygon).map(_.toGeoJson) == Some("{\"type\":\"MultiLineString\",\"coordinates\":[[[1.0,1.0],[3.0,1.0],[2.0,2.0],[1.0,1.0]],[[1.1,1.1],[2.5,1.1],[2.0,1.9],[1.1,1.1]]]}"))
    assert(Functions.ST_Boundary(collection) == None)
  }

}
