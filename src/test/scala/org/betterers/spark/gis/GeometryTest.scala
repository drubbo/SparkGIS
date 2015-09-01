package org.betterers.spark.gis

import org.apache.spark.sql.Row
import org.apache.spark.sql.test.TestSQLContext._
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.scalatest.FunSuite

/**
 * Basic tests for GIS UDTs
 */
class GeometryTest extends FunSuite {
  val point = Geometry.point((1.0, 2.0))
  val mPoint = Geometry.multiPoint((1.0, 1.0), (2.0, 0.5), (-1.0, 10.5))
  val line = Geometry.line((2.0, 1.0), (1.0, 0.0), (3.0, -1.0), (4.0, 5.0))
  val mLine = Geometry.multiLine(Seq((1.0, 2.0), (1.5, -3.0), (2.0, 3.0)), Seq((-10.0, 20.0), (20.0, 30.0)))
  val polygon = Geometry.polygon((1.0, 1.0), (2.0, 2.0), (3.0, 1.0), (2.0, 0.0))
  val mPoly = Geometry.multiPolygon(
    Seq((1.0, 1.0), (1.0, 2.0), (2.0, 2.0), (2.0, 1.0)),
    Seq((1.25, 1.25), (1.25, 1.75), (1.75, 1.75), (1.75, 1.25))
  )
  val coll = Geometry.aggregate(point, line, polygon)

  test("Centroid") {
    assertResult(Some(1.0, 2.0))(point.centroid.map(Utils.getCoordinates))
    assertResult(Some(0.6666666666666666, 4.0))(mPoint.centroid.map(Utils.getCoordinates))
    assertResult(Some(2.5, 1.25))(line.centroid.map(Utils.getCoordinates))
    assertResult(Some(4.093943723179785, 18.70075159924993))(mLine.centroid.map(Utils.getCoordinates))
    assertResult(Some(2.0, 1.0))(polygon.centroid.map(Utils.getCoordinates))
    assertResult(Some(1.5, 1.5))(mPoly.centroid.map(Utils.getCoordinates))
    assertResult(None)(coll.centroid)
  }

  test("Min coordinate") {
    assertResult(Some(1.0))(point.minCoordinate(_.getX))
    assertResult(Some(2.0))(point.minCoordinate(_.getY))

    assertResult(Some(-1.0))(mPoint.minCoordinate(_.getX))
    assertResult(Some(0.5))(mPoint.minCoordinate(_.getY))

    assertResult(Some(1.0))(line.minCoordinate(_.getX))
    assertResult(Some(-1.0))(line.minCoordinate(_.getY))

    assertResult(Some(-10.0))(mLine.minCoordinate(_.getX))
    assertResult(Some(-3.0))(mLine.minCoordinate(_.getY))

    assertResult(Some(1.0))(polygon.minCoordinate(_.getX))
    assertResult(Some(0.0))(polygon.minCoordinate(_.getY))

    assertResult(Some(1.0))(mPoly.minCoordinate(_.getX))
    assertResult(Some(1.0))(mPoly.minCoordinate(_.getY))

    assertResult(Some(1.0))(coll.minCoordinate(_.getX))
    assertResult(Some(-1.0))(coll.minCoordinate(_.getY))
  }

  test("Max coordinate") {
    assertResult(Some(1.0))(point.maxCoordinate(_.getX))
    assertResult(Some(2.0))(point.maxCoordinate(_.getY))

    assertResult(Some(2.0))(mPoint.maxCoordinate(_.getX))
    assertResult(Some(10.5))(mPoint.maxCoordinate(_.getY))

    assertResult(Some(4.0))(line.maxCoordinate(_.getX))
    assertResult(Some(5.0))(line.maxCoordinate(_.getY))

    assertResult(Some(20.0))(mLine.maxCoordinate(_.getX))
    assertResult(Some(30.0))(mLine.maxCoordinate(_.getY))

    assertResult(Some(3.0))(polygon.maxCoordinate(_.getX))
    assertResult(Some(2.0))(polygon.maxCoordinate(_.getY))

    assertResult(Some(2.0))(mPoly.maxCoordinate(_.getX))
    assertResult(Some(2.0))(mPoly.maxCoordinate(_.getY))

    assertResult(Some(4.0))(coll.maxCoordinate(_.getX))
    assertResult(Some(5.0))(coll.maxCoordinate(_.getY))
  }
}

