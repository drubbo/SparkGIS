package org.betterers.spark.gis.udf

import com.esri.core.geometry._
import com.esri.core.geometry.ogc._
import org.betterers.spark.gis.Geometry

/**
 * GIS functions as UDFs
 *
 * @author Ubik <emiliano.leporati@gmail.com>
 */
object Functions {

  /**
   * @param geom
   * @return the closure of the combinatorial boundary of this geometry;
   *         None if the input parameter is a GEOMETRYCOLLECTION
   */
  def ST_Boundary(geom: Geometry): Option[Geometry] =
    geom.ogc match {
      case c: OGCConcreteGeometryCollection => None
      case g => Some(Geometry(g.boundary()))
    }

  /**
   * @param geom
   * @return the coordinate dimension of the geometry
   */
  def ST_CoordDim(geom: Geometry): Int =
    geom.ogc.coordinateDimension()

  /**
   * @param geom
   * @return the inherent dimension of this geometry object,
   *         which must be less than or equal to the coordinate dimension
   */
  def ST_Dimension(geom: Geometry): Int =
    geom.ogc.dimension()

  /**
   * @param geom
   * @return the last point of a LINESTRING geometry as a POINT;
   *         None if the input parameter is not a LINESTRING
   */
  def ST_EndPoint(geom: Geometry): Option[Geometry] =
    geom.ogc match {
      case line: OGCLineString => Some(Geometry(line.endPoint()))
      case _ => None
    }

  /**
   * @param geom
   * @return The minimum bounding box for the supplied geometry
   */
  def ST_Envelope(geom: Geometry): Geometry =
    Geometry(geom.ogc.envelope())

  /**
   * @param geom
   * @return a line string representing the exterior ring of the POLYGON geometry;
   *         None if the geometry is not a polygon
   */
  def ST_ExteriorRing(geom: Geometry): Option[Geometry] =
    geom.ogc match {
      case poly: OGCPolygon => Some(Geometry(poly.exteriorRing()))
      case _ => None
    }

  /**
   * @param geom
   * @param n
   * @return the 1-based Nth geometry if the geometry is a GEOMETRYCOLLECTION, (MULTI)POINT, (MULTI)LINESTRING,
   *         MULTICURVE or (MULTI)POLYGON, POLYHEDRALSURFACE;
   *         None otherwise
   */
  def ST_GeometryN(geom: Geometry, n: Int): Option[Geometry] =
    geom.ogc match {
      case coll: OGCGeometryCollection => Some(Geometry(coll.geometryN(n)))
      case _ => None
    }

  /**
   * @param geom
   * @return the type of the geometry as a string. Eg: 'LINESTRING', 'POLYGON', 'MULTIPOINT', etc.
   */
  def ST_GeometryType(geom: Geometry): String =
    geom.ogc.geometryType().toUpperCase

  /**
   * @param geom
   * @param n
   * @return the 1-based Nth interior LINESTRING ring of the polygon geometry;
   *         None if the geometry is not a polygon or the given N is out of range
   */
  def ST_InteriorRingN(geom: Geometry, n: Int): Option[Geometry] =
    geom.ogc match {
      case poly: OGCPolygon => Some(Geometry(poly.interiorRingN(n)))
      case _ => None
    }

  /**
   * @param geom
   * @return true if each LINESTRING start and end points in the geometry are coincident
   */
  def ST_IsClosed(geom: Geometry): Boolean =
    geom.ogc match {
      case line: OGCCurve => line.isClosed
      case line: OGCMultiCurve => line.isClosed
      case _ => false
    }

  /**
   * @param geom
   * @return true if the geometry is a collection or a multi geometry
   */
  def ST_IsCollection(geom: Geometry): Boolean =
    geom.ogc.isInstanceOf[OGCGeometryCollection]

  /**
   *
   * @param geom
   * @return true if the geometry is empty
   */
  def ST_IsEmpty(geom: Geometry): Boolean =
    geom.ogc.isEmpty

  /**
   * @param geom
   * @return true if the LINESTRING geometry is closed and simple
   */
  def ST_IsRing(geom: Geometry): Boolean =
    geom.ogc match {
      case line: OGCCurve => line.isRing
      case _ => false
    }

  /**
   * @param geom
   * @return true if the geometry has no anomalous geometric points,
   *         such as self intersection or self tangency
   */
  def ST_IsSimple(geom: Geometry): Boolean =
    geom.ogc.isSimple

  /**
   * @param geom
   * @return the M coordinate of the POINT geometry, or None if not available or not a point
   */
  def ST_M(geom: Geometry): Option[Double] =
    geom.ogc match {
      case pt: OGCPoint => Some(pt.M)
      case _ => None
    }

  /**
   * @param geom
   * @return coordinate dimension of the geometry
   */
  def ST_NDims(geom: Geometry): Int =
    if (geom.ogc.is3D) 3
    else 2

  /**
   * @param geom
   * @return number of points (vertexes) in a geometry
   */
  def ST_NPoints(geom: Geometry): Int = {
    def count(cur: GeometryCursor): Int = {
      cur.next() match {
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
  def ST_NRings(geom: Geometry) =
    geom.numberOfRings

  /**
   * @param geom
   * @return if geometry is a GEOMETRYCOLLECTION (or MULTI*), the number of geometries;
   *         for single geometries, 1;
   *         None otherwise.
   */
  def ST_NumGeometries(geom: Geometry) =
    geom.numberOfGeometries

  /**
   * @param geom
   * @return the number of interior rings of the first polygon in the geometry
   */
  def ST_NumInteriorRings(geom: Geometry): Option[Int] =
    geom.numberOfRings.map(_ - 1)

  /**
   * Alias for [[ST_NumInteriorRings()]]
   * @param geom
   * @return the number of interior rings of the first polygon in the geometry
   */
  def ST_NumInteriorRing(geom: Geometry) =
    ST_NumInteriorRings(geom)

  /**
   * @param geom
   * @return the spatial reference identifier
   */
  def ST_SRID(geom: Geometry): Int =
    geom.srid

  /**
   * @param geom
   * @return the first point of a LINESTRING geometry as a POINT;
   *         None if the input parameter is not a LINESTRING
   */
  def ST_StartPoint(geom: Geometry): Option[Geometry] =
    geom.ogc match {
      case line: OGCLineString => Some(Geometry(line.startPoint()))
      case _ => None
    }

  /**
   * @param geom
   * @return the X coordinate of the POINT geometry;
   *         None if not available or not a point
   */
  def ST_X(geom: Geometry): Option[Double] =
    geom.ogc match {
      case pt: OGCPoint => Some(pt.X)
      case _ => None
    }

  /**
   * @param geom
   * @return maximum X coordinate of the geometry;
   *         None if empty geometry
   */
  def ST_XMax(geom: Geometry): Option[Double] =
    geom.maxCoordinate(_.getX)

  /**
   * @param geom
   * @return minimum X coordinate of the geometry;
   *         None if empty geometry
   */
  def ST_XMin(geom: Geometry): Option[Double] =
    geom.minCoordinate(_.getX)

  /**
   * @param geom
   * @return the Y coordinate of the POINT geometry;
   *         None if not available or not a point
   */
  def ST_Y(geom: Geometry): Option[Double] =
    geom.ogc match {
      case pt: OGCPoint => Some(pt.Y)
      case _ => None
    }

  /**
   * @param geom
   * @return maximum Y coordinate of the geometry;
   *         None if empty geometry
   */
  def ST_YMax(geom: Geometry): Option[Double] =
    geom.maxCoordinate(_.getY)

  /**
   * @param geom
   * @return minimum Y coordinate of the geometry;
   *         None if empty geometry
   */
  def ST_YMin(geom: Geometry): Option[Double] =
    geom.minCoordinate(_.getY)

  /**
   * @param geom
   * @return the Z coordinate of the POINT geometry;
   *         None if not available or not a point
   */
  def ST_Z(geom: Geometry): Option[Double] =
    geom.ogc match {
      case pt: OGCPoint => Some(pt.Z)
      case _ => None
    }

  /**
   * @param geom
   * @return maximum Z coordinate of the geometry;
   *         None if empty geometry
   */
  def ST_ZMax(geom: Geometry): Option[Double] =
    geom.maxCoordinate(_.getZ)

  /**
   * @param geom
   * @return minimum Z coordinate of the geometry;
   *         None if empty geometry
   */
  def ST_ZMin(geom: Geometry): Option[Double] =
    geom.minCoordinate(_.getZ)

  /**
   * @param geom
   * @return the area of the geometry if it is a POLYGON or MULTIPOLYGON
   */
  def ST_Area(geom: Geometry): Option[Double] =
    geom.ogc match {
      case s: OGCSurface => Some(s.area())
      case s: OGCMultiSurface => Some(s.area())
      case _ => None
    }

  /**
   * @param geom
   * @return geometry centroid
   */
  def ST_Centroid(geom: Geometry): Option[Geometry] =
  // NOTE still not supported by ESRI library
  // NOTE should support also other geometry types
  /* geom.ogc match {
    case c: OGCSurface => Some(Geometry(c.centroid()))
    case c: OGCMultiSurface => Some(Geometry(c.centroid()))
    case _ => None
  } */
    geom.centroid.map(new Geometry(_))

  /**
   * @param geomA
   * @param geomB
   * @return true if geomB is contained inside geomA
   */
  def ST_Contains(geomA: Geometry, geomB: Geometry): Boolean =
    geomA.ogc.contains(geomB.ogc)

  /**
   * @param geomA
   * @param geomB
   * @return true if the supplied geometries have some, but not all, interior points in common
   */
  def ST_Crosses(geomA: Geometry, geomB: Geometry): Boolean =
    geomA.ogc.crosses(geomB.ogc)

  /**
   * @param geomA
   * @param geomB
   * @return true if the supplied geometries do not share any space together
   */
  def ST_Disjoint(geomA: Geometry, geomB: Geometry): Boolean =
    geomA.ogc.disjoint(geomB.ogc)

  /**
   * @param geomA
   * @param geomB
   * @return the 2-dimensional cartesian minimum distance between two geometries in projected units
   */
  def ST_Distance(geomA: Geometry, geomB: Geometry): Double =
    geomA.ogc.distance(geomB.ogc)

  /**
   * @param geomA
   * @param geomB
   * @return true if the given geometries represent the same geometry
   */
  def ST_Equals(geomA: Geometry, geomB: Geometry): Boolean =
    geomA.ogc.equals(geomB)

  /**
   * @param geomA
   * @param geomB
   * @return true if the given geometries share any portion of space (are not disjoint)
   */
  def ST_Intersects(geomA: Geometry, geomB: Geometry): Boolean =
    geomA.ogc.intersects(geomB.ogc)

  /**
   * @param geom
   * @return the 2-dimensional length of a LINESTRING or MULTILINESTRING
   */
  def ST_Length(geom: Geometry): Option[Double] =
    geom.ogc match {
      case l: OGCCurve => Some(l.length)
      case l: OGCMultiCurve => Some(l.length)
      case _ => None
    }


}