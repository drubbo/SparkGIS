package org.betterers.spark.gis

import udf.Functions._

import scala.language.implicitConversions

/** Provides an implicit class to ''pimp'' the [[Geometry]] with
  * every functionality provided by [[udf.Functions]]
  *
  * Import it in your scope to enable the implicit conversion:
  * {{{
  *   import OperationalGeometry._
  *   val geom = Geometry.lime((10,10),(20,20),(10,30))
  *   if (geom.isClosed) println("that's weird")
  * }}}
  *
  * @author Ubik <emiliano.leporati@gmail.com>
  */
object OperationalGeometry {

  /** Augmented [[Geometry]] with accessors, transformations, operators and predicates
    * resembling every functionality provided by [[udf.Functions]]
    */
  implicit class OperationalGeometry(geom: Geometry) {

    /** Same as [[udf.Functions.ST_GeometryType]] */
    def geometryType = ST_GeometryType(geom)

    /** Same as [[udf.Functions.ST_SRID]] */
    def SRID = ST_SRID(geom)

    /** Same as [[udf.Functions.ST_NPoints]] */
    def numberOfPoints = ST_NPoints(geom)

    /** Same as [[udf.Functions.ST_NRings]] */
    def numberOfRings = ST_NRings(geom)

    /** Same as [[udf.Functions.ST_NDims]] */
    def numberOfDimensions = ST_NDims(geom)

    /** Same as [[udf.Functions.ST_NumGeometries]] */
    def numberOfSubGeometries = ST_NumGeometries(geom)

    /** Same as [[udf.Functions.ST_NumInteriorRings]] */
    def numberOfInteriorRings = ST_NumInteriorRings(geom)

    /** Same as [[udf.Functions.ST_IsClosed]] */
    def isClosed = ST_IsClosed(geom)

    /** Same as [[udf.Functions.ST_IsCollection]] */
    def isCollection = ST_IsCollection(geom)

    /** Same as [[udf.Functions.ST_IsEmpty]] */
    def isEmpty = ST_IsEmpty(geom)

    /** Same as [[udf.Functions.ST_IsRing]] */
    def isRing = ST_IsRing(geom)

    /** Same as [[udf.Functions.ST_IsSimple]] */
    def isSimple = ST_IsSimple(geom)

    /** Same as [[udf.Functions.ST_CoordDim]] */
    def coordinateDimension = ST_CoordDim(geom)

    /** Same as [[udf.Functions.ST_Dimension]] */
    def dimension = ST_Dimension(geom)

    /** Same as [[udf.Functions.ST_Boundary]] */
    def boundary = ST_Boundary(geom)

    /** Same as [[udf.Functions.ST_Envelope]] */
    def envelope = ST_Envelope(geom)

    /** Same as [[udf.Functions.ST_Buffer]] */
    def buffer(distance: Double) = ST_Buffer(geom, distance)

    /** Same as [[udf.Functions.ST_ConvexHull]] */
    def convexHull = ST_ConvexHull(geom)

    /** Same as [[udf.Functions.ST_Simplify]] */
    def simplified = ST_Simplify(geom)

    /** Same as [[udf.Functions.ST_ExteriorRing]] */
    def exteriorRing = ST_ExteriorRing(geom)

    /** Same as [[udf.Functions.ST_StartPoint]] */
    def startPoint = ST_StartPoint(geom)

    /** Same as [[udf.Functions.ST_EndPoint]] */
    def endPoint = ST_EndPoint(geom)

    /** Same as [[udf.Functions.ST_Area]] */
    def area = ST_Area(geom)

    /** Same as [[udf.Functions.ST_Length]] */
    def length = ST_Length(geom)

    /** Same as [[udf.Functions.ST_Centroid]] */
    def centroid = ST_Centroid(geom)

    /** Same as [[udf.Functions.ST_Distance]] */
    def distanceTo(other: Geometry) = ST_Distance(geom, other)

    /** Same as [[udf.Functions.ST_Difference]] */
    def minus(other: Geometry) = ST_Difference(geom, other)

    /** Same as [[udf.Functions.ST_Intersection]] */
    def intersection(other: Geometry) = ST_Intersection(geom, other)

    /** Same as [[udf.Functions.ST_SymDifference]] */
    def symmetricDifference(other: Geometry) = ST_SymDifference(geom, other)

    /** Same as [[udf.Functions.ST_Union]] */
    def union(other: Geometry) = ST_Union(geom, other)

    /** Same as [[udf.Functions.ST_Relate]] */
    def relate(other: Geometry, matrix: String) = ST_Relate(geom, other, matrix)

    /** Same as [[udf.Functions.ST_Distance]] */
    def <->(other: Geometry) = ST_Distance(geom, other)

    /** Same as [[udf.Functions.ST_Difference]] */
    def \(other: Geometry) = ST_Difference(geom, other)

    /** Same as [[udf.Functions.ST_Union]] */
    def +(other: Geometry) = ST_Union(geom, other)

    /** Same as [[udf.Functions.ST_Intersection]] */
    def ^(other: Geometry) = ST_Intersection(geom, other)

    /** Same as [[udf.Functions.ST_SymDifference]] */
    def |\|(other: Geometry) = ST_SymDifference(geom, other)

    /** Same as [[udf.Functions.ST_GeometryN]] */
    def subGeometry(n: Int) = ST_GeometryN(geom, n)

    /** Same as [[udf.Functions.ST_InteriorRingN]] */
    def interiorRing(n: Int) = ST_InteriorRingN(geom, n)

    /** Same as [[udf.Functions.ST_PointOnSurface]] */
    def pointOnSurface = ST_PointOnSurface(geom)

    /** Same as [[udf.Functions.ST_M]] */
    def m = ST_M(geom)

    /** Same as [[udf.Functions.ST_X]] */
    def x = ST_X(geom)

    /** Same as [[udf.Functions.ST_XMin]] */
    def xMin = ST_XMin(geom)

    /** Same as [[udf.Functions.ST_XMax]] */
    def xMax = ST_XMax(geom)

    /** Same as [[udf.Functions.ST_Y]] */
    def y = ST_Y(geom)

    /** Same as [[udf.Functions.ST_YMin]] */
    def yMin = ST_YMin(geom)

    /** Same as [[udf.Functions.ST_YMax]] */
    def yMax = ST_YMax(geom)

    /** Same as [[udf.Functions.ST_Z]] */
    def z = ST_Z(geom)

    /** Same as [[udf.Functions.ST_ZMin]] */
    def zMin = ST_ZMin(geom)

    /** Same as [[udf.Functions.ST_ZMax]] */
    def zMax = ST_ZMax(geom)

    /** Same as [[udf.Functions.ST_Contains]] */
    def contains(other: Geometry) = ST_Contains(geom, other)

    /** Same as [[udf.Functions.ST_Crosses]] */
    def crosses(other: Geometry) = ST_Crosses(geom, other)

    /** Same as [[udf.Functions.ST_Disjoint]] */
    def disjointFrom(other: Geometry) = ST_Disjoint(geom, other)

    /** Same as [[udf.Functions.ST_Equals]] */
    def equals(other: Geometry) = ST_Equals(geom, other)

    /** Same as [[udf.Functions.ST_Intersects]] */
    def intersects(other: Geometry) = ST_Intersects(geom, other)

    /** Same as [[udf.Functions.ST_Overlaps]] */
    def overlaps(other: Geometry) = ST_Overlaps(geom, other)

    /** Same as [[udf.Functions.ST_Touches]] */
    def touches(other: Geometry) = ST_Touches(geom, other)

    /** Same as [[udf.Functions.ST_Within]] */
    def within(other: Geometry) = ST_Within(geom, other)

    /** Same as [[udf.Functions.ST_Contains]] */
    def >(other: Geometry) = ST_Contains(geom, other)

    /** Same as [[udf.Functions.ST_Within]] */
    def <(other: Geometry) = ST_Within(geom, other)

    /** Same as [[udf.Functions.ST_Crosses]] */
    def ><(other: Geometry) = ST_Crosses(geom, other)

    /** Same as [[udf.Functions.ST_Disjoint]] */
    def <>(other: Geometry) = ST_Disjoint(geom, other)

    /** Same as [[udf.Functions.ST_Equals]] */
    def ==(other: Geometry) = ST_Equals(geom, other)

    /** Same as [[udf.Functions.ST_Equals]] */
    def !=(other: Geometry) = !ST_Equals(geom, other)

    /** Same as [[udf.Functions.ST_Intersects]] */
    def |^|(other: Geometry) = ST_Intersects(geom, other)

    /** Same as [[udf.Functions.ST_Overlaps]] */
    def |@|(other: Geometry) = ST_Overlaps(geom, other)

    /** Same as [[udf.Functions.ST_Touches]] */
    def ||(other: Geometry) = ST_Touches(geom, other)

    /** Same as [[udf.Functions.ST_AsBinary]] */
    def asBinary = ST_AsBinary(geom)

    /** Same as [[udf.Functions.ST_AsEWKT]] */
    def asText = ST_AsEWKT(geom)

    /** Same as [[udf.Functions.ST_AsGeoJSON]] */
    def asGeoJson = ST_AsGeoJSON(geom)

    /** Same as [[udf.Functions.ST_AsEWKT]] */
    override def toString = asText
  }
}
