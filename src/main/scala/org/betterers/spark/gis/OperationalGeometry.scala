package org.betterers.spark.gis

import udf.Functions._

/**
 * Augmented [[Geometry]] with accessors, transformations, and predicates
 * resembling every functionality provided by [[udf.Functions]]
 *
 * @author Ubik <emiliano.leporati@gmail.com>
 */
class OperationalGeometry(geom: Geometry) {

  /**
   * @see [[ST_GeometryType]]
   */
  def geometryType = ST_GeometryType(geom)
  /**
   * @see [[ST_SRID]]
   */
  def SRID = ST_SRID(geom)

  /**
   * @see [[ST_NPoints]]
   */
  def numberOfPoints = ST_NPoints(geom)
  /**
   * @see [[ST_NRings]]
   */
  def numberOfRings = ST_NRings(geom)
  /**
   * @see [[ST_NDims]]
   */
  def numberOfDimensions = ST_NDims(geom)
  /**
   * @see [[ST_NumGeometries]]
   */
  def numberOfSubGeometries = ST_NumGeometries(geom)
  /**
   * @see [[ST_NumInteriorRings]]
   */
  def numberOfInteriorRings = ST_NumInteriorRings(geom)

  /**
   * @see [[ST_IsClosed]]
   */
  def isClosed = ST_IsClosed(geom)
  /**
   * @see [[ST_IsCollection]]
   */
  def isCollection = ST_IsCollection(geom)
  /**
   * @see [[ST_IsEmpty]]
   */
  def isEmpty = ST_IsEmpty(geom)
  /**
   * @see [[ST_IsRing]]
   */
  def isRing = ST_IsRing(geom)
  /**
   * @see [[ST_IsSimple]]
   */
  def isSimple = ST_IsSimple(geom)

  /**
   * @see [[ST_CoordDim]]
   */
  def coordinateDimension = ST_CoordDim(geom)
  /**
   * @see [[ST_Dimension]]
   */
  def dimension = ST_Dimension(geom)

  /**
   * @see [[ST_Boundary]]
   */
  def boundary = ST_Boundary(geom)
  /**
   * @see [[ST_Envelope]]
   */
  def envelope = ST_Envelope(geom)
  /**
   * @see [[ST_Buffer]]
   */
  def buffer(distance: Double) = ST_Buffer(geom, distance)
  /**
   * @see [[ST_ConvexHull]]
   */
  def convexHull = ST_ConvexHull(geom)
  /**
   * @see [[ST_Simplify]]
   */
  def simplified = ST_Simplify(geom)

  /**
   * @see [[ST_ExteriorRing]]
   */
  def exteriorRing = ST_ExteriorRing(geom)
  /**
   * @see [[ST_StartPoint]]
   */
  def startPoint = ST_StartPoint(geom)
  /**
   * @see [[ST_EndPoint]]
   */
  def endPoint = ST_EndPoint(geom)
  /**
   * @see [[ST_Area]]
   */
  def area = ST_Area(geom)
  /**
   * @see [[ST_Length]]
   */
  def length = ST_Length(geom)
  /**
   * @see [[ST_Centroid]]
   */
  def centroid = ST_Centroid(geom)

  /**
   * @see [[ST_Distance]]
   */
  def distanceTo(other: Geometry) = ST_Distance(geom, other)
  /**
   * @see [[ST_Difference]]
   */
  def minus(other: Geometry) = ST_Difference(geom, other)
  /**
   * @see [[ST_Intersection]]
   */
  def intersection(other: Geometry) = ST_Intersection(geom, other)
  /**
   * @see [[ST_SymDifference]]
   */
  def symmetricDifference(other: Geometry) = ST_SymDifference(geom, other)
  /**
   * @see [[ST_Union]]
   */
  def union(other: Geometry) = ST_Union(geom, other)
  /**
   * @see [[ST_Relate]]
   */
  def relate(other: Geometry, matrix: String) = ST_Relate(geom, other, matrix)

  /**
   * @see [[ST_Distance]]
   */
  def <->(other: Geometry) = ST_Distance(geom, other)
  /**
   * @see [[ST_Difference]]
   */
  def \(other: Geometry) = ST_Difference(geom, other)
  /**
   * @see [[ST_Union]]
   */
  def +(other: Geometry) = ST_Union(geom, other)
  /**
   * @see [[ST_Intersection]]
   */
  def ^(other: Geometry) = ST_Intersection(geom, other)
  /**
   * @see [[ST_SymDifference]]
   */
  def |\|(other: Geometry) = ST_SymDifference(geom, other)

  /**
   * @see [[ST_GeometryN]]
   */
  def subGeometry(n: Int) = ST_GeometryN(geom, n)
  /**
   * @see [[ST_InteriorRingN]]
   */
  def interiorRing(n: Int) = ST_InteriorRingN(geom, n)
  /**
   * @see [[ST_PointOnSurface]]
   */
  def pointOnSurface = ST_PointOnSurface(geom)

  /**
   * @see [[ST_M]]
   */
  def m = ST_M(geom)
  /**
   * @see [[ST_X]]
   */
  def x = ST_X(geom)
  /**
   * @see [[ST_XMin]]
   */
  def xMin = ST_XMin(geom)
  /**
   * @see [[ST_XMax]]
   */
  def xMax = ST_XMax(geom)
  /**
   * @see [[ST_Y]]
   */
  def y = ST_Y(geom)
  /**
   * @see [[ST_YMin]]
   */
  def yMin = ST_YMin(geom)
  /**
   * @see [[ST_YMax]]
   */
  def yMax = ST_YMax(geom)
  /**
   * @see [[ST_Z]]
   */
  def z = ST_Z(geom)
  /**
   * @see [[ST_ZMin]]
   */
  def zMin = ST_ZMin(geom)
  /**
   * @see [[ST_ZMax]]
   */
  def zMax = ST_ZMax(geom)

  /**
   * @see [[ST_Contains]]
   */
  def contains(other: Geometry) = ST_Contains(geom, other)
  /**
   * @see [[ST_Crosses]]
   */
  def crosses(other: Geometry) = ST_Crosses(geom, other)
  /**
   * @see [[ST_Disjoint]]
   */
  def disjointFrom(other: Geometry) = ST_Disjoint(geom, other)
  /**
   * @see [[ST_Equals]]
   */
  def equals(other: Geometry) = ST_Equals(geom, other)
  /**
   * @see [[ST_Intersects]]
   */
  def intersects(other: Geometry) = ST_Intersects(geom, other)
  /**
   * @see [[ST_Overlaps]]
   */
  def overlaps(other: Geometry) = ST_Overlaps(geom, other)
  /**
   * @see [[ST_Touches]]
   */
  def touches(other: Geometry) = ST_Touches(geom, other)
  /**
   * @see [[ST_Within]]
   */
  def within(other: Geometry) = ST_Within(geom, other)

  /**
   * @see [[ST_Contains]]
   */
  def >(other: Geometry) = ST_Contains(geom, other)
  /**
   * @see [[ST_Within]]
   */
  def <(other: Geometry) = ST_Within(geom, other)
  /**
   * @see [[ST_Crosses]]
   */
  def ><(other: Geometry) = ST_Crosses(geom, other)
  /**
   * @see [[ST_Disjoint]]
   */
  def <>(other: Geometry) = ST_Disjoint(geom, other)
  /**
   * @see [[ST_Equals]]
   */
  def ==(other: Geometry) = ST_Equals(geom, other)
  /**
   * @see [[ST_Equals]]
   */
  def !=(other: Geometry) = !ST_Equals(geom, other)
  /**
   * @see [[ST_Intersects]]
   */
  def |^|(other: Geometry) = ST_Intersects(geom, other)
  /**
   * @see [[ST_Overlaps]]
   */
  def |@|(other: Geometry) = ST_Overlaps(geom, other)
  /**
   * @see [[ST_Touches]]
   */
  def ||(other: Geometry) = ST_Touches(geom, other)

  /**
   * @see [[ST_AsBinary]]
   */
  def asBinary = ST_AsBinary(geom)
  /**
   * @see [[ST_AsEWKT]]
   */
  def asText = ST_AsEWKT(geom)
  /**
   * @see [[ST_AsGeoJSON]]
   */
  def asGeoJson = ST_AsGeoJSON(geom)
  /**
   * @see [[ST_AsEWKT]]
   */
  override def toString = asText

}

/**
 * Implicit conversion from [[Geometry]]
 *
 * @author Ubik <emiliano.leporati@gmail.com>
 */
object OperationalGeometry {
  implicit def fromGeometry(geometry: Geometry): Unit = {
    new OperationalGeometry(geometry)
  }
}