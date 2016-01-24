package org.betterers.spark.gis

import org.scalatest.FunSuite

/**
 * Basic tests for GIS UDTs
 */
class GeometryTest extends FunSuite {
  import Geometry.WGS84

  val point = Geometry.point((1.0, 2.0))
  val mPoint = Geometry.multiPoint((1.0, 1.0), (2.0, 0.5), (-1.0, 10.5))
  val line = Geometry.line((2.0, 1.0), (1.0, 0.0), (3.0, -1.0), (4.0, 5.0))
  val mLine = Geometry.multiLine(Seq((1.0, 2.0), (1.5, -3.0), (2.0, 3.0)), Seq((-10.0, 20.0), (20.0, 30.0)))
  val polygon = Geometry.polygon((1.0, 1.0), (2.0, 2.0), (3.0, 1.0), (2.0, 0.0))
  val mPoly = Geometry.multiPolygon(
    Seq((1.0, 1.0), (1.0, 2.0), (2.0, 2.0), (2.0, 1.0)),
    Seq((1.25, 1.25), (1.25, 1.75), (1.75, 1.75), (1.75, 1.25))
  )
  val coll = Geometry.collection(point, line, polygon)

  test("Transform") {
    import Geometry.ImplicitConversions._

    val MonteMario = 26592
    def round(x: Double): Double =
      (x * 100000).round / 100000.0

    val turinCenter = Geometry.point(7.684628, 45.071413)
    assert(turinCenter.srid == Geometry.WGS84)
    val turinCenterMM: Geometry = turinCenter.transformed(MonteMario)
    assertResult("POINT")(turinCenterMM.geometryType)
    assertResult(Some(4493440.377831126))(turinCenterMM.xMax)
    assertResult(Some(890996.5508925163))(turinCenterMM.yMax)
    val turinCenterBack: Geometry = turinCenterMM.transformed(Geometry.WGS84)
    assertResult(Some(7.68463))(turinCenterBack.xMax.map(round))
    assertResult(Some(45.0714))(turinCenterBack.yMax.map(round))

    val turinArea = Geometry.polygon((7.635404, 45.126668), (7.751662, 45.090807), 
      (7.694542, 45.007853), (7.545411, 45.043281), (7.565195, 45.103410))
    val turinAreaMM: Geometry = turinArea.transformed(MonteMario)
    assertResult("POLYGON")(turinAreaMM.geometryType)
    assertResult(Some(898856.4223355835))(turinAreaMM.yMax)
    assertResult(Some(4486049.327615481))(turinAreaMM.xMin)

    for(x <- Seq(point, mPoint, line, mLine, polygon, mPoly, coll)) {
      val t: Geometry = x.transformed(MonteMario)
      assertResult(Geometry.WGS84)(x.srid)
      assertResult(MonteMario)(t.srid)
      assertResult(x.geometryType)(t.geometryType)
    }
  }

  test("Min coordinate") {
    import Geometry.ImplicitConversions._

    assertResult(Some(1.0))(point.xMin)
    assertResult(Some(2.0))(point.yMin)

    assertResult(Some(-1.0))(mPoint.xMin)
    assertResult(Some(0.5))(mPoint.yMin)

    assertResult(Some(1.0))(line.xMin)
    assertResult(Some(-1.0))(line.yMin)

    assertResult(Some(-10.0))(mLine.xMin)
    assertResult(Some(-3.0))(mLine.yMin)

    assertResult(Some(1.0))(polygon.xMin)
    assertResult(Some(0.0))(polygon.yMin)

    assertResult(Some(1.0))(mPoly.xMin)
    assertResult(Some(1.0))(mPoly.yMin)

    assertResult(Some(1.0))(coll.xMin)
    assertResult(Some(-1.0))(coll.yMin)
  }

  test("Max coordinate") {
    import Geometry.ImplicitConversions._

    assertResult(Some(1.0))(point.xMax)
    assertResult(Some(2.0))(point.yMax)

    assertResult(Some(2.0))(mPoint.xMax)
    assertResult(Some(10.5))(mPoint.yMax)

    assertResult(Some(4.0))(line.xMax)
    assertResult(Some(5.0))(line.yMax)

    assertResult(Some(20.0))(mLine.xMax)
    assertResult(Some(30.0))(mLine.yMax)

    assertResult(Some(3.0))(polygon.xMax)
    assertResult(Some(2.0))(polygon.yMax)

    assertResult(Some(2.0))(mPoly.xMax)
    assertResult(Some(2.0))(mPoly.yMax)

    assertResult(Some(4.0))(coll.xMax)
    assertResult(Some(5.0))(coll.yMax)
  }

  test("Implicit conversion") {
    import Geometry.ImplicitConversions._

    val l = Geometry.line((10.0,10.0), (20.0,20.0), (10.0,30.0))
    assertResult(false)(l.isClosed)
    val p: Option[Geometry] = l.startPoint
    assertResult(false)(p.isEmpty)
    assertResult(10.0)(p.get.x.get)
  }

  test("Operators") {
    import GeometryOperators._
    import Geometry.ImplicitConversions._

    val l1 = Geometry.line((10.0,10.0), (20.0,20.0), (10.0,30.0))
    val l2 = Geometry.line((20.0,20.0), (30.0,30.0), (40.0,40.0))
    assertResult(l1.distance(l2))(l1 <-> l2)
  }

  test("Pattern matching") {
    def sum(c: Coordinate*): Double =
      c.foldLeft(0.0) { case (acc, (x, y)) => acc + x + y }

    assertResult(true)(point match {
      case GisPoint(p) => true
    })
    assertResult(3.0)(point match {
      case GisPoint(Coordinate(x, y)) => x + y
    })

    assertResult(true)(mPoint match {
      case GisMultiPoint(p) => true
    })
    assertResult(14.0)(mPoint match {
      case GisMultiPoint(Coordinate(a +: b +: c +: Nil)) => sum(a, b, c)
    })
    assertResult(true)(mPoint match {
      case GisGeometryCollection(p) => true
    })

    assertResult(true)(line match {
      case GisLineString(p) => true
    })
    assertResult(15.0)(line match {
      case GisLineString(Coordinate(a +: b +: c +: d +: Nil)) => sum(a, b, c, d)
    })

    assertResult(true)(mLine match {
      case GisMultiLineString(p) => true
    })
    assertResult(true)(mLine match {
      case GisGeometryCollection(p) => true
    })
    assertResult((3, 60.0))(mLine match {
      case GisMultiLineString(Coordinate(l1 +: (a +: b +: Nil) +: Nil)) =>
        (l1.length, sum(a, b))
    })

    assertResult(true)(polygon match {
      case GisPolygon(p) => true
    })
    assertResult(5)(polygon match {
      case GisPolygon(Coordinate(r1 +: Nil)) => r1.length
    })

    assertResult(true)(mPoly match {
      case GisMultiPolygon(p) => true
    })
    assertResult((14.0, 5))(mPoly match {
      case GisMultiPolygon(Coordinate((r1p1 +: Nil) +: (r1p2 +: Nil) +: Nil)) =>
        (sum(r1p1: _*), r1p2.length)
    })
    assertResult(true)(mPoly match {
      case GisGeometryCollection(p) => true
    })

    assertResult(true)(coll match {
      case GisGeometryCollection(p) => true
    })
    assertResult(true)(coll match {
      case GisGeometryCollection(GisGeometry((_: GisPoint) +: (_: GisLineString) +: (_: GisPolygon) +: Nil)) => true
    })
  }
}

