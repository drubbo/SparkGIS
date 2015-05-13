package org.betterers.spark.gis.udf

import com.esri.core.geometry._
import com.esri.core.geometry.ogc._
import org.betterers.spark.gis.GeometryValue

object Functions {

  /**
   * @param geom
   * @return the closure of the combinatorial boundary of this geometry
   */
  def ST_Boundary(geom: GeometryValue): GeometryValue =
    new GeometryValue(geom.ogc.boundary())

  /**
   * @param geom
   * @return the coordinate dimension of the geometry
   */
  def ST_CoordDim(geom: GeometryValue): Int =
    geom.ogc.coordinateDimension()

  /**
   * @param geom
   * @return the inherent dimension of this geometry object,
   *         which must be less than or equal to the coordinate dimension
   */
  def ST_Dimension(geom: GeometryValue): Int =
    geom.ogc.dimension()

  /**
   * @param geom
   * @return the last point of a LINESTRING geometry as a POINT;
   *         NULL if the input parameter is not a LINESTRING
   */
  def ST_EndPoint(geom: GeometryValue): GeometryValue =
    geom.ogc match {
      case line: OGCLineString =>
        new GeometryValue(line.endPoint())
      case _ =>
        null
    }

  /**
   * @param geom
   * @return The minimum bounding box for the supplied geometry
   */
  def ST_Envelope(geom: GeometryValue): GeometryValue =
    new GeometryValue(geom.ogc.envelope())

  /**
   * @param geom
   * @return a line string representing the exterior ring of the POLYGON geometry;
   *         NULL if the geometry is not a polygon
   */
  def ST_ExteriorRing(geom: GeometryValue): GeometryValue =
    geom.ogc match {
      case poly: OGCPolygon =>
        new GeometryValue(poly.exteriorRing())
      case _ =>
        null
    }

  /**
   * @param geom
   * @param n
   * @return the 1-based Nth geometry if the geometry is a GEOMETRYCOLLECTION, (MULTI)POINT, (MULTI)LINESTRING,
   *         MULTICURVE or (MULTI)POLYGON, POLYHEDRALSURFACE;
   *         NULL otherwise
   */
  def ST_GeometryN(geom: GeometryValue, n: Int): GeometryValue =
    geom.ogc match {
      case coll: OGCGeometryCollection =>
        new GeometryValue(coll.geometryN(n))
      case _ =>
        null
    }

  /**
   * @param geom
   * @return the type of the geometry as a string. Eg: 'LINESTRING', 'POLYGON', 'MULTIPOINT', etc.
   */
  def ST_GeometryType(geom: GeometryValue): String =
    geom.ogc.geometryType().toUpperCase()

  /**
   * @param geom
   * @param n
   * @return the 1-based Nth interior LINESTRING ring of the polygon geometry;
   *         NULL if the geometry is not a polygon or the given N is out of range
   */
  def ST_InteriorRingN(geom: GeometryValue, n: Int): GeometryValue =
    geom.ogc match {
      case poly: OGCPolygon =>
        new GeometryValue(poly.interiorRingN(n))
      case _ =>
        null
    }

  /**
   * @param geom
   * @return true if each LINESTRING start and end points in the geometry are coincident
   */
  def ST_IsClosed(geom: GeometryValue): Boolean =
    geom.ogc match {
      case line: OGCCurve =>
        line.isClosed
      case multi: OGCMultiCurve =>
        multi.isClosed
      case _ =>
        false
    }

  /**
   * @param geom
   * @return true if the geometry is a collection or a multi geometry
   */
  def ST_IsCollection(geom: GeometryValue): Boolean =
    geom.ogc.isInstanceOf[OGCGeometryCollection]

  /**
   *
   * @param geom
   * @return true if the geometry is empty
   */
  def ST_IsEmpty(geom: GeometryValue): Boolean =
    geom.ogc.isEmpty

  /**
   * @param geom
   * @return true if the LINESTRING geometry is closed and simple
   */
  def ST_IsRing(geom: GeometryValue): Boolean =
    geom.ogc match {
      case line: OGCCurve =>
        line.isRing
      case _ =>
        false
    }

  /**
   * @param geom
   * @return true if the geometry has no anomalous geometric points,
   *         such as self intersection or self tangency
   */
  def ST_IsSimple(geom: GeometryValue): Boolean =
    geom.ogc.isSimple

  /**
   * @param geom
   * @return the M coordinate of the POINT geometry, or NULL if not available or not a point
   */
  def ST_M(geom: GeometryValue) =
    geom.ogc match {
      case pt: OGCPoint => pt.M
      case _ => null
    }

  /**
   * @param geom
   * @return coordinate dimension of the geometry
   */
  def ST_NDims(geom: GeometryValue): Int =
    if (geom.ogc.is3D) 3
    else 2

  /**
   * @param geom
   * @return number of points (vertexes) in a geometry
   */
  def ST_NPoints(geom: GeometryValue): Int = {
    def count(cur: GeometryCursor): Int = {
      cur.next match {
        case null => 0
        case g: MultiVertexGeometry => g.getPointCount + count(cur)
        case g: Line => 2 + count(cur)
        case g: Point => 1 + count(cur)
      }
    }
    count(geom.ogc.getEsriGeometryCursor)
  }

  /**
   * @param geom
   * @return number of rings if the geometry is a polygon or multi-polygon
   */
  def ST_NRings(geom: GeometryValue) =
    geom.numberOfRings

  /**
   * @param geom
   * @return if geometry is a GEOMETRYCOLLECTION (or MULTI*), the number of geometries;
   *         for single geometries, 1;
   *         NULL otherwise.
   */
  def ST_NumGeometries(geom: GeometryValue) =
    geom.numberOfGeometries

  /**
   * @param geom
   * @return the number of interior rings of the first polygon in the geometry
   */
  def ST_NumInteriorRings(geom: GeometryValue) =
    geom.numberOfRings match {
      case n: Int =>
        n - 1
      case _ =>
        null
    }

  /**
   * Alias for [[ST_NumInteriorRings()]]
   * @param geom
   * @return the number of interior rings of the first polygon in the geometry
   */
  def ST_NumInteriorRing(geom: GeometryValue) =
    ST_NumInteriorRings(geom)

  /**
   * @param geom
   * @return the spatial reference identifier
   */
  def ST_SRID(geom: GeometryValue): Int =
    geom.srid

  /**
   * @param geom
   * @return the first point of a LINESTRING geometry as a POINT;
   *         NULL if the input parameter is not a LINESTRING
   */
  def ST_StartPoint(geom: GeometryValue): GeometryValue =
    geom.ogc match {
      case line: OGCLineString =>
        new GeometryValue(line.startPoint())
      case _ =>
        null
    }

  /**
   * @param geom
   * @return the X coordinate of the POINT geometry, or NULL if not available or not a point
   */
  def ST_X(geom: GeometryValue) =
    geom.ogc match {
      case pt: OGCPoint => pt.X
      case _ => null
    }

  /**
   * @param geom
   * @return maximum X coordinate of the geometry, NULL if empty geometry
   */
  def ST_XMax(geom: GeometryValue) =
    geom.maxCoordinate(_.getX)

  /**
   * @param geom
   * @return minimum X coordinate of the geometry, NULL if empty geometry
   */
  def ST_XMin(geom: GeometryValue) =
    geom.minCoordinate(_.getX)

  /**
   * @param geom
   * @return the Y coordinate of the POINT geometry, or NULL if not available or not a point
   */
  def ST_Y(geom: GeometryValue) =
    geom.ogc match {
      case pt: OGCPoint => pt.Y
      case _ => null
    }

  /**
   * @param geom
   * @return maximum Y coordinate of the geometry, NULL if empty geometry
   */
  def ST_YMax(geom: GeometryValue) =
    geom.maxCoordinate(_.getY)

  /**
   * @param geom
   * @return minimum Y coordinate of the geometry, NULL if empty geometry
   */
  def ST_YMin(geom: GeometryValue) =
    geom.minCoordinate(_.getY)

  /**
   * @param geom
   * @return the Z coordinate of the POINT geometry, or NULL if not available or not a point
   */
  def ST_Z(geom: GeometryValue) =
    geom.ogc match {
      case pt: OGCPoint => pt.Z
      case _ => null
    }

  /**
   * @param geom
   * @return maximum Z coordinate of the geometry, NULL if empty geometry
   */
  def ST_ZMax(geom: GeometryValue) =
    geom.maxCoordinate(_.getZ)

  /**
   * @param geom
   * @return minimum Z coordinate of the geometry, NULL if empty geometry
   */
  def ST_ZMin(geom: GeometryValue) =
    geom.minCoordinate(_.getZ)

}