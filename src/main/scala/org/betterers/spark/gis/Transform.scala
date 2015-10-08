package org.betterers.spark.gis

import com.esri.core.geometry.{Polygon, Polyline, MultiPoint, Point}
import com.vividsolutions.jts.geom.{Coordinate, GeometryFactory}
import org.geotools.geometry.jts.JTS
import org.geotools.referencing.CRS

/** Functions to convert geometries between coordinate reference systems */
private[gis]
object Transform {

  // type of point in GeoTools
  private type GTPoint = com.vividsolutions.jts.geom.Point

  // factory for points in GeoTools
  private lazy val gtGeometryFactory = new GeometryFactory()

  // conversion from ESRI point to GeoTools point
  private def point2gt(p: Point): GTPoint =
    gtGeometryFactory.createPoint(new Coordinate(p.getX, p.getY))

  /** Returns a function able to convert points from a coordinate reference system to another */
  private def forPoint(sourceSRID: Int, targetSRID: Int): Point => Geometry.Coordinates = {
    assert(sourceSRID != targetSRID)
    // get coordinate reference systems
    val fromRS = CRS.decode(s"EPSG:$sourceSRID")
    val toRS = CRS.decode(s"EPSG:$targetSRID")

    // find transformation and bind it in a lambda
    val transformation = CRS.findMathTransform(fromRS, toRS, false)
    val doTransform: GTPoint => GTPoint =
      JTS.transform(_, transformation).asInstanceOf[GTPoint]

    // result is a function that transform the input point into another one
    // stepping through a GeoTools Coordinate
    p => {
      val gtCoord = doTransform(point2gt(p)).getCoordinate
      (gtCoord.getOrdinate(0), gtCoord.getOrdinate(1))
    }
  }

  import GeometryBuilder._

  /** Given a point transformation obtained calling [[forPoint]], returns a function
    * that will use it to transform full [[ESRIGeometry]]
    */
  private def forESRIGeometry(tx: Point => Geometry.Coordinates): ESRIGeometry => ESRIGeometry = {
    case p: Point =>
      mkPoint(tx(p))
    case p: MultiPoint =>
      mkMultiPoint(Utils.getPoints(p).map(tx))
    case p: Polyline if p.getPathCount <= 1 =>
      mkLine(Utils.getPoints(p, 0).map(tx))
    case p: Polyline =>
      mkMultiLine(Range(0, p.getPathCount).map(Utils.getPoints(p, _).map(tx)))
    case p: Polygon if p.getPathCount <= 1 =>
      mkPolygon(Utils.getPoints(p, 0).map(tx))
    case p: Polygon =>
      mkMultiPolygon(Range(0, p.getPathCount).map(Utils.getPoints(p, _).map(tx)))
  }

  /** Transforms the input geometry coordinates to a different coordinate reference system */
  def apply(geom: Geometry, targetSRID: Int): Geometry =
    if (geom.srid == targetSRID)
      geom
    else {
      val tx = forESRIGeometry(forPoint(geom.srid, targetSRID))
      new Geometry(targetSRID, geom.geometries.map(tx))
    }

}
