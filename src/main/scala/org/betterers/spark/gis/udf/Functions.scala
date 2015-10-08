package org.betterers.spark.gis.udf

import com.esri.core.geometry._
import com.esri.core.geometry.ogc._
import org.apache.spark.sql.SQLContext
import org.betterers.spark.gis.Geometry

/** GIS functions over [[Geometry]] values as Spark UDFs
 *
 * Register them in a SQLContext in order to use them with the provided method:
 * {{{
 *   Functions.register(sqlCtx)
 * }}}
 *
 * @author Ubik <emiliano.leporati@gmail.com>
 */
object Functions {

  /** Returns the closure of the combinatorial boundary of this geometry,
    * or `None` if the input parameter is a `GEOMETRYCOLLECTION`
   */
  def ST_Boundary(geom: Geometry): Option[Geometry] =
    geom.ogc match {
      case c: OGCConcreteGeometryCollection => None
      case g => Some(Geometry(g.boundary()))
    }

  /** Returns the coordinate dimension of the geometry */
  def ST_CoordDim(geom: Geometry): Int =
    geom.ogc.coordinateDimension()

  /** Returns the inherent dimension of this geometry object,
    * which must be less than or equal to the coordinate dimension
    */
  def ST_Dimension(geom: Geometry): Int =
    geom.ogc.dimension()

  /** Returns the last point of a `LINESTRING` geometry as a `POINT`;
    * `None` if the input parameter is not a `LINESTRING`
    */
  def ST_EndPoint(geom: Geometry): Option[Geometry] =
    geom.ogc match {
      case line: OGCLineString => Some(Geometry(line.endPoint()))
      case _ => None
    }

  /** Returns the minimum bounding box for the supplied geometry */
  def ST_Envelope(geom: Geometry): Geometry =
    Geometry(geom.ogc.envelope())

  /** Returns a line string representing the exterior ring of the `POLYGON` geometry;
    * `None` if the geometry is not a polygon
    */
  def ST_ExteriorRing(geom: Geometry): Option[Geometry] =
    geom.ogc match {
      case poly: OGCPolygon => Some(Geometry(poly.exteriorRing()))
      case _ => None
    }

  /** Returns the 1-based Nth geometry if the geometry is a `GEOMETRYCOLLECTION`,
    * `POINT`, `MULTIPOINT`, `LINESTRING`, `MULTILINESTRING`, `MULTICURVE`, `POLYGON`
    * or `MULTIPOLYGON`; `None` otherwise
    */
  def ST_GeometryN(geom: Geometry, n: Int): Option[Geometry] =
    geom.ogc match {
      case coll: OGCGeometryCollection => Some(Geometry(coll.geometryN(n)))
      case _ => None
    }

  /** Returns the type of the geometry as a string. Eg: `"LINESTRING"`, `"POLYGON"`, etc. */
  def ST_GeometryType(geom: Geometry): String =
    geom.ogc.geometryType().toUpperCase

  /** Returns the 1-based Nth interior `LINESTRING` ring of the polygon geometry;
    * `None` if the geometry is not a polygon or the given N is out of range
    */
  def ST_InteriorRingN(geom: Geometry, n: Int): Option[Geometry] =
    geom.ogc match {
      case poly: OGCPolygon => Some(Geometry(poly.interiorRingN(n)))
      case _ => None
    }

  /** Returns true if each `LINESTRING` start and end points in the geometry are coincident */
  def ST_IsClosed(geom: Geometry): Boolean =
    geom.ogc match {
      case line: OGCCurve => line.isClosed
      case line: OGCMultiCurve => line.isClosed
      case _ => false
    }

  /** Returns true if the geometry is a collection or a multi geometry */
  def ST_IsCollection(geom: Geometry): Boolean =
    geom.ogc.isInstanceOf[OGCGeometryCollection]

  /** Returns true if the geometry is empty */
  def ST_IsEmpty(geom: Geometry): Boolean =
    geom.ogc.isEmpty

  /** Returns true if the `LINESTRING` geometry is closed and simple */
  def ST_IsRing(geom: Geometry): Boolean =
    geom.ogc match {
      case line: OGCCurve => line.isRing
      case _ => false
    }

  /** Returns true if the geometry has no anomalous geometric points,
    * such as self intersection or self tangency
    */
  def ST_IsSimple(geom: Geometry): Boolean =
    geom.ogc.isSimple

  /** Returns the M coordinate of the `POINT` geometry, or `None` if not available or not a point */
  def ST_M(geom: Geometry): Option[Double] =
    geom.ogc match {
      case pt: OGCPoint => Some(pt.M)
      case _ => None
    }

  /** Returns the coordinate dimension of the geometry */
  def ST_NDims(geom: Geometry): Int =
    if (geom.ogc.is3D) 3
    else 2

  /** Returns the number of points (vertexes) in a geometry */
  def ST_NPoints(geom: Geometry): Int = {
    def count(cur: GeometryCursor): Int = {
      cur.next() match {
        case null => 0
        case g: Point => 1 + count(cur)
        case g: Line => 2 + count(cur)
        case g: MultiVertexGeometry => g.getPointCount + count(cur)
      }
    }
    count(geom.ogc.getEsriGeometryCursor)
  }

  /** Returns the number of rings if the geometry is a polygon or multi-polygon */
  def ST_NRings(geom: Geometry) =
    geom.numberOfRings

  /** Returns the number of geometries for `GEOMETRYCOLLECTION` or `MULTI*` geometries,
    * 1 for single geometries
    */
  def ST_NumGeometries(geom: Geometry) =
    geom.numberOfGeometries

  /** Returns the number of interior rings of the first polygon in the geometry */
  def ST_NumInteriorRings(geom: Geometry): Option[Int] =
    geom.numberOfRings.map(_ - 1)

  /** Alias for [[ST_NumInteriorRings]] */
  def ST_NumInteriorRing(geom: Geometry) =
    ST_NumInteriorRings(geom)

  /** Returns the spatial reference identifier of a geometry */
  def ST_SRID(geom: Geometry): Int =
    geom.srid

  /** Returns a new geometry with its coordinates transformed to the SRID referenced by the integer parameter */
  def ST_Transform(geom: Geometry, targetSRID: Int): Geometry =
    geom.transformTo(targetSRID)

  /** Returns the first point of a `LINESTRING` geometry as a `POINT`;
    * `None` if the input parameter is not a `LINESTRING`
    */
  def ST_StartPoint(geom: Geometry): Option[Geometry] =
    geom.ogc match {
      case line: OGCLineString => Some(Geometry(line.startPoint()))
      case _ => None
    }

  /** Returns the X coordinate of the `POINT` geometry or `None` if not available or not a point */
  def ST_X(geom: Geometry): Option[Double] =
    geom.ogc match {
      case pt: OGCPoint => Some(pt.X)
      case _ => None
    }

  /** Returns maximum X coordinate of the geometry or `None` if empty geometry */
  def ST_XMax(geom: Geometry): Option[Double] =
    geom.maxCoordinate(_.getX)

  /** Returns minimum X coordinate of the geometry or `None` if empty geometry */
  def ST_XMin(geom: Geometry): Option[Double] =
    geom.minCoordinate(_.getX)

  /** Returns the Y coordinate of the `POINT` geometry; or `None` if not a point */
  def ST_Y(geom: Geometry): Option[Double] =
    geom.ogc match {
      case pt: OGCPoint => Some(pt.Y)
      case _ => None
    }

  /** Returns maximum Y coordinate of the geometry or `None` if empty geometry */
  def ST_YMax(geom: Geometry): Option[Double] =
    geom.maxCoordinate(_.getY)

  /** Returns minimum Y coordinate of the geometry or `None` if empty geometry */
  def ST_YMin(geom: Geometry): Option[Double] =
    geom.minCoordinate(_.getY)

  /** Returns the Z coordinate of the `POINT` geometry or `None` if not a point */
  def ST_Z(geom: Geometry): Option[Double] =
    geom.ogc match {
      case pt: OGCPoint => Some(pt.Z)
      case _ => None
    }

  /** Returns maximum Z coordinate of the geometry or `None` if empty geometry */
  def ST_ZMax(geom: Geometry): Option[Double] =
    geom.maxCoordinate(_.getZ)

  /** Returns minimum Z coordinate of the geometry or `None` if empty geometry */
  def ST_ZMin(geom: Geometry): Option[Double] =
    geom.minCoordinate(_.getZ)

  /** Returns the area of the geometry if it is a `POLYGON` or `MULTIPOLYGON` */
  def ST_Area(geom: Geometry): Option[Double] =
    geom.ogc match {
      case s: OGCSurface => Some(s.area())
      case s: OGCMultiSurface => Some(s.area())
      case _ => None
    }

  /** Returns geometry centroid */
  def ST_Centroid(geom: Geometry): Option[Geometry] =
  // NOTE still not supported by ESRI library
  // NOTE should support also other geometry types
  /* geom.ogc match {
    case c: OGCSurface => Some(Geometry(c.centroid()))
    case c: OGCMultiSurface => Some(Geometry(c.centroid()))
    case _ => None
  } */
    geom.centroid.map(new Geometry(_))

  /** Returns true if geometry B is contained inside geometry A */
  def ST_Contains(geomA: Geometry, geomB: Geometry): Boolean =
    geomA.ogc.contains(geomB.ogc)

  /** Returns true if the supplied geometries have some, but not all, interior points in common */
  def ST_Crosses(geomA: Geometry, geomB: Geometry): Boolean =
    geomA.ogc.crosses(geomB.ogc)

  /** Returns true if the supplied geometries do not share any space together */
  def ST_Disjoint(geomA: Geometry, geomB: Geometry): Boolean =
    geomA.ogc.disjoint(geomB.ogc)

  /** Returns the 2-dimensional cartesian minimum distance between two geometries in projected units */
  def ST_Distance(geomA: Geometry, geomB: Geometry): Double =
    geomA.ogc.distance(geomB.ogc)

  /** Returns true if the given geometries represent the same geometry */
  def ST_Equals(geomA: Geometry, geomB: Geometry): Boolean =
    geomA.ogc.equals(geomB.ogc)

  /** Returns true if the given geometries share any portion of space (are not disjoint) */
  def ST_Intersects(geomA: Geometry, geomB: Geometry): Boolean =
    geomA.ogc.intersects(geomB.ogc)

  /** Returns the 2-dimensional length of a `LINESTRING` or `MULTILINESTRING` */
  def ST_Length(geom: Geometry): Option[Double] =
    geom.ogc match {
      case l: OGCCurve => Some(l.length)
      case l: OGCMultiCurve => Some(l.length)
      case _ => None
    }

  /** Returns true if the geometries share space, are of the same dimension,
    * but are not completely contained by each other
    */
  def ST_Overlaps(geomA: Geometry, geomB: Geometry): Boolean =
    geomA.ogc.overlaps(geomB.ogc)

  /** Returns a `POINT` guaranteed to lie on the surface */
  def ST_PointOnSurface(geom: Geometry): Option[Geometry] =
    geom.ogc match {
      case s: OGCSurface => Some(Geometry(s.pointOnSurface()))
      case s: OGCMultiSurface => Some(Geometry(s.pointOnSurface()))
      case _ => None
    }

  /** Returns true if the geometries have at least one point in common, but their interiors do not intersect */
  def ST_Touches(geomA: Geometry, geomB: Geometry): Boolean =
    geomA.ogc.overlaps(geomB.ogc)

  /** Returns true if geometry A is completely inside geometry B */
  def ST_Within(geomA: Geometry, geomB: Geometry): Boolean =
    geomA.ogc.overlaps(geomB.ogc)

  /** Returns a geometry that represents all points whose distance from this geometry
    *  is less than or equal to distance
    */
  def ST_Buffer(geom: Geometry, distance: Double): Geometry =
    Geometry(geom.ogc.buffer(distance))

  /** Returns the minimum convex geometry that encloses all geometries within the set */
  def ST_ConvexHull(geom: Geometry): Geometry =
    Geometry(geom.ogc.convexHull())

  /** Returns a geometry that represents that part of geometry A that does not intersect with geometry B */
  def ST_Difference(geomA: Geometry, geomB: Geometry): Geometry =
    Geometry(geomA.ogc.difference(geomB.ogc))

  /** Returns a geometry that represents the shared portion of two geometries */
  def ST_Intersection(geomA: Geometry, geomB: Geometry): Geometry =
    Geometry(geomA.ogc.intersection(geomB.ogc))

  /** Returns a simplified version of the given geometry */
  def ST_Simplify(geom: Geometry): Geometry =
    geom.ogc match {
      case g: OGCConcreteGeometryCollection =>
        val x: Seq[Geometry] = geom.geometries.map(new Geometry(_))
        Geometry.aggregate(x.map(ST_Simplify): _*)
      case g =>
        Geometry(g.makeSimple)
    }

  /** Returns a geometry that represents the portions of the given geometries that do not intersect */
  def ST_SymDifference(geomA: Geometry, geomB: Geometry): Geometry =
    Geometry(geomA.ogc.symDifference(geomB.ogc))

  /** Returns a geometry that represents the point set union of the given geometries */
  def ST_Union(geomA: Geometry, geomB: Geometry): Geometry =
    Geometry(geomA.ogc.union(geomB.ogc))

  /** Returns true if geometry A is spatially related to geometry B, by testing for intersections
    * as specified by the values in the intersection matrix
    */
  def ST_Relate(geomA: Geometry, geomB: Geometry, matrix: String): Boolean =
    geomA.ogc.relate(geomB.ogc, matrix)

  /** Returns the `WKB` representation of a geometry */
  def ST_AsBinary(geom: Geometry): Array[Byte] =
    geom.toBinary

  /** Returns the `WKB` representation of a geometry */
  def ST_AsEWKB(geom: Geometry): Array[Byte] =
    geom.toBinary

  /** Returns a geometry decoded from the input `WKB` */
  def ST_GeomFromBinary(wkb: Array[Byte]): Geometry =
    Geometry.fromBinary(wkb)

  /** Returns a geometry decoded from the input `WKB` */
  def ST_GeomFromEWKB(wkb: Array[Byte]): Geometry =
    Geometry.fromBinary(wkb)

  /** Returns the `WKT` representation of a geometry */
  def ST_AsText(geom: Geometry): String =
    geom.toString

  /** Returns the `WKT` representation of a geometry */
  def ST_AsEWKT(geom: Geometry): String =
    geom.toString

  /** Returns a geometry decoded from the input `WKT` */
  def ST_GeomFromText(wkt: String): Geometry =
    Geometry.fromString(wkt)

  /** Returns a geometry decoded from the input `WKT` */
  def ST_GeomFromEWKT(wkt: String): Geometry =
    Geometry.fromString(wkt)

  /** Returns the `GeoJSON` representation of a geometry */
  def ST_AsGeoJSON(geom: Geometry): String =
    geom.toGeoJson

  /** Returns a geometry decoded from the input `GeoJSON` */
  def ST_GeomFromGeoJSON(json: String): Geometry =
    Geometry.fromGeoJson(json)

  /** Registers every GIS function in a SQL context */
  def register(ctx: SQLContext): Unit = {
    ctx.udf.register("ST_Boundary", ST_Boundary(_: Geometry))
    ctx.udf.register("ST_CoordDim", ST_CoordDim(_: Geometry))
    ctx.udf.register("ST_Dimension", ST_Dimension(_: Geometry))
    ctx.udf.register("ST_EndPoint", ST_EndPoint(_: Geometry))
    ctx.udf.register("ST_Envelope", ST_Envelope(_: Geometry))
    ctx.udf.register("ST_ExteriorRing", ST_ExteriorRing(_: Geometry))
    ctx.udf.register("ST_GeometryN", ST_GeometryN(_: Geometry, _: Int))
    ctx.udf.register("ST_GeometryType", ST_GeometryType(_: Geometry))
    ctx.udf.register("ST_InteriorRingN", ST_InteriorRingN(_: Geometry, _: Int))
    ctx.udf.register("ST_IsClosed", ST_IsClosed(_: Geometry))
    ctx.udf.register("ST_IsCollection", ST_IsCollection(_: Geometry))
    ctx.udf.register("ST_IsEmpty", ST_IsEmpty(_: Geometry))
    ctx.udf.register("ST_IsRing", ST_IsRing(_: Geometry))
    ctx.udf.register("ST_IsSimple", ST_IsSimple(_: Geometry))
    ctx.udf.register("ST_M", ST_M(_: Geometry))
    ctx.udf.register("ST_NDims", ST_NDims(_: Geometry))
    ctx.udf.register("ST_NPoints", ST_NPoints(_: Geometry))
    ctx.udf.register("ST_NRings", ST_NRings(_: Geometry))
    ctx.udf.register("ST_NumGeometries", ST_NumGeometries(_: Geometry))
    ctx.udf.register("ST_NumInteriorRings", ST_NumInteriorRings(_: Geometry))
    ctx.udf.register("ST_NumInteriorRing", ST_NumInteriorRing(_: Geometry))
    ctx.udf.register("ST_SRID", ST_SRID(_: Geometry))
    ctx.udf.register("ST_Transform", ST_Transform(_: Geometry, _: Int))
    ctx.udf.register("ST_StartPoint", ST_StartPoint(_: Geometry))
    ctx.udf.register("ST_X", ST_X(_: Geometry))
    ctx.udf.register("ST_XMax", ST_XMax(_: Geometry))
    ctx.udf.register("ST_XMin", ST_XMin(_: Geometry))
    ctx.udf.register("ST_Y", ST_Y(_: Geometry))
    ctx.udf.register("ST_YMax", ST_YMax(_: Geometry))
    ctx.udf.register("ST_YMin", ST_YMin(_: Geometry))
    ctx.udf.register("ST_Z", ST_Z(_: Geometry))
    ctx.udf.register("ST_ZMax", ST_ZMax(_: Geometry))
    ctx.udf.register("ST_ZMin", ST_ZMin(_: Geometry))
    ctx.udf.register("ST_Area", ST_Area(_: Geometry))
    ctx.udf.register("ST_Centroid", ST_Centroid(_: Geometry))
    ctx.udf.register("ST_Contains", ST_Contains(_: Geometry, _: Geometry))
    ctx.udf.register("ST_Crosses", ST_Crosses(_: Geometry, _: Geometry))
    ctx.udf.register("ST_Disjoint", ST_Disjoint(_: Geometry, _: Geometry))
    ctx.udf.register("ST_Distance", ST_Distance(_: Geometry, _: Geometry))
    ctx.udf.register("ST_Equals", ST_Equals(_: Geometry, _: Geometry))
    ctx.udf.register("ST_Intersects", ST_Intersects(_: Geometry, _: Geometry))
    ctx.udf.register("ST_Length", ST_Length(_: Geometry))
    ctx.udf.register("ST_Overlaps", ST_Overlaps(_: Geometry, _: Geometry))
    ctx.udf.register("ST_PointOnSurface", ST_PointOnSurface(_: Geometry))
    ctx.udf.register("ST_Touches", ST_Touches(_: Geometry, _: Geometry))
    ctx.udf.register("ST_Within", ST_Within(_: Geometry, _: Geometry))
    ctx.udf.register("ST_Buffer", ST_Buffer(_: Geometry, _: Double))
    ctx.udf.register("ST_ConvexHull", ST_ConvexHull(_: Geometry))
    ctx.udf.register("ST_Difference", ST_Difference(_: Geometry, _: Geometry))
    ctx.udf.register("ST_Intersection", ST_Intersection(_: Geometry, _: Geometry))
    ctx.udf.register("ST_Simplify", ST_Simplify(_: Geometry))
    ctx.udf.register("ST_SymDifference", ST_SymDifference(_: Geometry, _: Geometry))
    ctx.udf.register("ST_Union", ST_Union(_: Geometry, _: Geometry))
    ctx.udf.register("ST_Relate", ST_Relate(_: Geometry, _: Geometry, _: String))
    ctx.udf.register("ST_AsBinary", ST_AsBinary(_: Geometry))
    ctx.udf.register("ST_AsEWKB", ST_AsEWKB(_: Geometry))
    ctx.udf.register("ST_AsText", ST_AsText(_: Geometry))
    ctx.udf.register("ST_AsEWKT", ST_AsEWKT(_: Geometry))
    ctx.udf.register("ST_AsGeoJSON", ST_AsGeoJSON(_: Geometry))
    ctx.udf.register("ST_GeomFromBinary", ST_GeomFromBinary(_: Array[Byte]))
    ctx.udf.register("ST_GeomFromEWKB", ST_GeomFromEWKB(_: Array[Byte]))
    ctx.udf.register("ST_GeomFromText", ST_GeomFromText(_: String))
    ctx.udf.register("ST_GeomFromEWKT", ST_GeomFromEWKT(_: String))
    ctx.udf.register("ST_GeomFromGeoJSON", ST_GeomFromGeoJSON(_: String))
  }
}
