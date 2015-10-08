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

  test("Transform") {
    import OperationalGeometry._
    val MonteMario = 26592
    def round(x: Double): Double =
      (x * 100000).round / 100000.0

    val turinCenter = Geometry.point(7.684628, 45.071413)
    val turinCenterMM = turinCenter.transformTo(MonteMario)
    assertResult("POINT")(turinCenterMM.geometryType)
    assertResult(Some(4493440.377831126))(turinCenterMM.xMax)
    assertResult(Some(890996.5508925163))(turinCenterMM.yMax)
    val turinCenterBack = turinCenterMM.transformTo(Geometry.WGS84)
    assertResult(Some(7.68463))(turinCenterBack.xMax.map(round))
    assertResult(Some(45.0714))(turinCenterBack.yMax.map(round))

    val turinArea = Geometry.polygon((7.635404, 45.126668), (7.751662, 45.090807), 
      (7.694542, 45.007853), (7.545411, 45.043281), (7.565195, 45.103410))
    val turinAreaMM = turinArea.transformTo(MonteMario)
    assertResult("POLYGON")(turinAreaMM.geometryType)
    assertResult(Some(898856.4223355835))(turinAreaMM.yMax)
    assertResult(Some(4486049.327615481))(turinAreaMM.xMin)

    for(x <- Seq(point, mPoint, line, mLine, polygon, mPoly, coll)) {
      val t = x.transformTo(MonteMario)
      assertResult(Geometry.WGS84)(x.srid)
      assertResult(MonteMario)(t.srid)
      assertResult(x.geometryType)(t.geometryType)
    }
  }

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

  test("Implicit conversion") {
    import OperationalGeometry._

    val g = Geometry.line((10.0,10.0), (20.0,20.0), (10.0,30.0))
    assertResult(false)(g.isClosed)
  }
}

