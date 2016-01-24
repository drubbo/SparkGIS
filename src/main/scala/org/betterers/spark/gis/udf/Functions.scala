package org.betterers.spark.gis.udf

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

  import Geometry.ImplicitConversions._

  /** Returns the closure of the combinatorial boundary of this geometry,
    * or `None` if the input parameter is a `GEOMETRYCOLLECTION`
    */
  def ST_Boundary(geom: Geometry): Option[Geometry] =
    geom.boundary

  /** Returns the coordinate dimension of the geometry */
  def ST_CoordDim(geom: Geometry): Int =
    geom.coordinateDimension

  /** Returns the inherent dimension of this geometry object,
    * which must be less than or equal to the coordinate dimension
    */
  def ST_Dimension(geom: Geometry): Int =
    geom.dimension

  /** Returns the last point of a `LINESTRING` geometry as a `POINT`;
    * `None` if the input parameter is not a `LINESTRING`
    */
  def ST_EndPoint(geom: Geometry): Option[Geometry] =
    geom.endPoint

  /** Returns the minimum bounding box for the supplied geometry */
  def ST_Envelope(geom: Geometry): Geometry =
    geom.envelope

  /** Returns a line string representing the exterior ring of the `POLYGON` geometry;
    * `None` if the geometry is not a polygon
    */
  def ST_ExteriorRing(geom: Geometry): Option[Geometry] =
    geom.exteriorRing

  /** Returns the 1-based Nth geometry if the geometry is a `GEOMETRYCOLLECTION`,
    * `POINT`, `MULTIPOINT`, `LINESTRING`, `MULTILINESTRING`, `MULTICURVE`, `POLYGON`
    * or `MULTIPOLYGON`; `None` otherwise
    */
  def ST_GeometryN(geom: Geometry, n: Int): Option[Geometry] =
    geom.subGeometry(n - 1)

  /** Returns the type of the geometry as a string. Eg: `"LINESTRING"`, `"POLYGON"`, etc. */
  def ST_GeometryType(geom: Geometry): String =
    geom.geometryType

  /** Returns the 1-based Nth interior `LINESTRING` ring of the polygon geometry;
    * `None` if the geometry is not a polygon or the given N is out of range
    */
  def ST_InteriorRingN(geom: Geometry, n: Int): Option[Geometry] =
    geom.interiorRing(n - 1)

  /** Returns true if each `LINESTRING` start and end points in the geometry are coincident */
  def ST_IsClosed(geom: Geometry): Boolean =
    geom.isClosed

  /** Returns true if the geometry is a collection or a multi geometry */
  def ST_IsCollection(geom: Geometry): Boolean =
    geom.isCollection

  /** Returns true if the geometry is empty */
  def ST_IsEmpty(geom: Geometry): Boolean =
    geom.isEmpty

  /** Returns true if the `LINESTRING` geometry is closed and simple */
  def ST_IsRing(geom: Geometry): Boolean =
    geom.isRing

  /** Returns true if the geometry has no anomalous geometric points,
    * such as self intersection or self tangency
    */
  def ST_IsSimple(geom: Geometry): Boolean =
    geom.isSimple

  /** Returns the M coordinate of the `POINT` geometry, or `None` if not available or not a point */
  def ST_M(geom: Geometry): Option[Double] =
    geom.m

  /** Returns the coordinate dimension of the geometry */
  def ST_NDims(geom: Geometry): Int =
    geom.dimension

  /** Returns the number of points (vertexes) in a geometry */
  def ST_NPoints(geom: Geometry): Int =
    geom.numPoints

  /** Returns the number of rings if the geometry is a polygon or multi-polygon */
  def ST_NRings(geom: Geometry): Option[Int] =
    geom.numRings

  /** Returns the number of geometries for `GEOMETRYCOLLECTION` or `MULTI*` geometries,
    * 1 for single geometries
    */
  def ST_NumGeometries(geom: Geometry) =
    geom.numGeometries

  /** Returns the number of interior rings of the first polygon in the geometry */
  def ST_NumInteriorRings(geom: Geometry): Option[Int] =
    geom.numInteriorRings

  /** Alias for [[ST_NumInteriorRings]] */
  def ST_NumInteriorRing(geom: Geometry): Option[Int] =
    geom.numInteriorRings

  /** Returns the spatial reference identifier of a geometry */
  def ST_SRID(geom: Geometry): Int =
    geom.srid

  /** Returns a new geometry with its coordinates transformed to the SRID referenced by the integer parameter */
  def ST_Transform(geom: Geometry, targetSRID: Int): Geometry =
    geom.transformed(targetSRID)

  /** Returns the first point of a `LINESTRING` geometry as a `POINT`;
    * `None` if the input parameter is not a `LINESTRING`
    */
  def ST_StartPoint(geom: Geometry): Option[Geometry] =
    geom.startPoint

  /** Returns the X coordinate of the `POINT` geometry or `None` if not available or not a point */
  def ST_X(geom: Geometry): Option[Double] =
    geom.x

  /** Returns maximum X coordinate of the geometry or `None` if empty geometry */
  def ST_XMax(geom: Geometry): Option[Double] =
    geom.xMax

  /** Returns minimum X coordinate of the geometry or `None` if empty geometry */
  def ST_XMin(geom: Geometry): Option[Double] =
    geom.xMin

  /** Returns the Y coordinate of the `POINT` geometry; or `None` if not a point */
  def ST_Y(geom: Geometry): Option[Double] =
    geom.y

  /** Returns maximum Y coordinate of the geometry or `None` if empty geometry */
  def ST_YMax(geom: Geometry): Option[Double] =
    geom.yMax

  /** Returns minimum Y coordinate of the geometry or `None` if empty geometry */
  def ST_YMin(geom: Geometry): Option[Double] =
    geom.yMin

  /** Returns the Z coordinate of the `POINT` geometry or `None` if not a point */
  def ST_Z(geom: Geometry): Option[Double] =
    geom.z

  /** Returns maximum Z coordinate of the geometry or `None` if empty geometry */
  def ST_ZMax(geom: Geometry): Option[Double] =
    geom.zMax

  /** Returns minimum Z coordinate of the geometry or `None` if empty geometry */
  def ST_ZMin(geom: Geometry): Option[Double] =
    geom.zMin

  /** Returns the area of the geometry if it is a `POLYGON` or `MULTIPOLYGON` */
  def ST_Area(geom: Geometry): Option[Double] =
    geom.area

  /** Returns geometry centroid */
  def ST_Centroid(geom: Geometry): Option[Geometry] =
    geom.centroid

  /** Returns true if geometry B is contained inside geometry A */
  def ST_Contains(geomA: Geometry, geomB: Geometry): Boolean =
    geomA.contains(geomB)

  /** Returns true if the supplied geometries have some, but not all, interior points in common */
  def ST_Crosses(geomA: Geometry, geomB: Geometry): Boolean =
    geomA.crosses(geomB)

  /** Returns true if the supplied geometries do not share any space together */
  def ST_Disjoint(geomA: Geometry, geomB: Geometry): Boolean =
    geomA.disjoint(geomB)

  /** Returns the 2-dimensional cartesian minimum distance between two geometries in projected units */
  def ST_Distance(geomA: Geometry, geomB: Geometry): Double =
    geomA.distance(geomB)

  /** Returns true if the given geometries represent the same geometry */
  def ST_Equals(geomA: Geometry, geomB: Geometry): Boolean =
    geomA.equals(geomB)

  /** Returns true if the given geometries share any portion of space (are not disjoint) */
  def ST_Intersects(geomA: Geometry, geomB: Geometry): Boolean =
    geomA.intersects(geomB)

  /** Returns the 2-dimensional length of a `LINESTRING` or `MULTILINESTRING` */
  def ST_Length(geom: Geometry): Option[Double] =
    geom.length

  /** Returns the perimeter of a `POLYGON` or `MULTIPOLYGON` */
  def ST_Perimeter(geom: Geometry): Option[Double] =
    geom.perimeter

  /** Returns true if the geometries share space, are of the same dimension,
    * but are not completely contained by each other
    */
  def ST_Overlaps(geomA: Geometry, geomB: Geometry): Boolean =
    geomA.overlaps(geomB)

  /** Returns a `POINT` guaranteed to lie on the surface */
  def ST_PointOnSurface(geom: Geometry): Option[Geometry] =
    geom.interiorPoint

  /** Returns true if the geometries have at least one point in common, but their interiors do not intersect */
  def ST_Touches(geomA: Geometry, geomB: Geometry): Boolean =
    geomA.touches(geomB)

  /** Returns true if geometry A is completely inside geometry B */
  def ST_Within(geomA: Geometry, geomB: Geometry): Boolean =
    geomA.within(geomB)

  /** Returns a geometry that represents all points whose distance from this geometry
    * is less than or equal to distance
    */
  def ST_Buffer(geom: Geometry, distance: Double): Geometry =
    geom.buffer(distance)

  /** Returns the minimum convex geometry that encloses all geometries within the set */
  def ST_ConvexHull(geom: Geometry): Geometry =
    geom.convexHull

  /** Returns a geometry that represents that part of geometry A that does not intersect with geometry B */
  def ST_Difference(geomA: Geometry, geomB: Geometry): Geometry =
    geomA.difference(geomB)

  /** Returns a geometry that represents the shared portion of two geometries */
  def ST_Intersection(geomA: Geometry, geomB: Geometry): Geometry =
    geomA.intersection(geomB)

  /** Returns a simplified version of the given geometry */
  def ST_Simplify(geom: Geometry, tolerance: Double): Geometry =
    geom.simplified(tolerance)

  /** Returns a geometry that represents the portions of the given geometries that do not intersect */
  def ST_SymDifference(geomA: Geometry, geomB: Geometry): Geometry =
    geomA.symmetricDifference(geomB)

  /** Returns a geometry that represents the point set union of the given geometries */
  def ST_Union(geomA: Geometry, geomB: Geometry): Geometry =
    geomA.union(geomB)

  /** Returns true if geometry A is spatially related to geometry B, by testing for intersections
    * as specified by the values in the intersection matrix
    */
  def ST_Relate(geomA: Geometry, geomB: Geometry, matrix: String): Boolean =
    geomA.relate(geomB, matrix)

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
    ctx.udf.register("ST_Perimeter", ST_Perimeter(_: Geometry))
    ctx.udf.register("ST_Overlaps", ST_Overlaps(_: Geometry, _: Geometry))
    ctx.udf.register("ST_PointOnSurface", ST_PointOnSurface(_: Geometry))
    ctx.udf.register("ST_Touches", ST_Touches(_: Geometry, _: Geometry))
    ctx.udf.register("ST_Within", ST_Within(_: Geometry, _: Geometry))
    ctx.udf.register("ST_Buffer", ST_Buffer(_: Geometry, _: Double))
    ctx.udf.register("ST_ConvexHull", ST_ConvexHull(_: Geometry))
    ctx.udf.register("ST_Difference", ST_Difference(_: Geometry, _: Geometry))
    ctx.udf.register("ST_Intersection", ST_Intersection(_: Geometry, _: Geometry))
    ctx.udf.register("ST_Simplify", ST_Simplify(_: Geometry, _: Double))
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
