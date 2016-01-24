package org.betterers.spark.gis

import java.io.StringWriter

import com.vividsolutions.jts.geom.{Coordinate => Coord, Geometry => Geom, _}
import com.vividsolutions.jts.io.{WKBWriter, WKTWriter}
import com.vividsolutions.jts.simplify.TopologyPreservingSimplifier
import org.geotools.geojson.geom.GeometryJSON
import org.geotools.geometry.jts.JTS
import org.geotools.referencing.CRS

import scala.language.implicitConversions

/** Wrapper for GeoTools geometry providing a consistent interface across various geometry types.
  *
  * Subclasses of [[GisGeometry]] implement specific behaviour for the geometry type they wrap.
  */
class GisGeometry(val geom: Geom, private val _srid: Option[Int] = None)
  extends Serializable {

  import GisGeometry.ImplicitConversions.fromGeom

  _srid.foreach(geom.setSRID)

  /** Utility to return None if the wrapped geometry is empty, or something otherwise */
  protected[gis]
  def orElse[R](ifNotEmpty: => R): Option[R] =
    if (geom.isEmpty) None
    else Option(ifNotEmpty)

  /** The spatial reference ID of this geometry */
  val srid: Int = geom.getSRID

  /** Returns a copy of this geometry with another SRID */
  final def withSRID(newSRID: Int): GisGeometry = {
    val clone = geom.clone().asInstanceOf[Geom]
    clone.setSRID(newSRID)
    GisGeometry(clone)
  }

  /** The type of this geometry */
  final def geometryType = geom.getGeometryType.toUpperCase

  /** Number of points in this geometry */
  final def numPoints: Int = geom.getNumPoints

  @transient
  private[gis] 
  lazy val allCoordinates =
    (0 until numPoints).map(i => geom.getCoordinates.apply(i))

  /** Returns a coordinate of the enclosed geometries satisfying some predicate
    *
    * @param extract Function to extract the required coordinate from a point
    * @param predicate Predicate to satisfy (be the greatest, least, etc)
    */
  private final def getCoordinate(extract: Coord => Double,
                                  predicate: (Double, Double) => Boolean): Option[Double] = {
    def reduce(a: Double, b: Double) = if (predicate(a, b)) a else b
    orElse(allCoordinates.map(extract).reduceLeft(reduce))
  }

  /** Returns the X coordinate of this geometry if it's a [[GisPoint]] */
  def x: Option[Double] = None

  /** Returns the max X coordinate among this geometry points */
  final def xMax: Option[Double] = getCoordinate(_.x, _ > _)

  /** Returns the min X coordinate among this geometry points */
  final def xMin: Option[Double] = getCoordinate(_.x, _ < _)

  /** Returns the Y coordinate of this geometry if it's a [[GisPoint]] */
  def y: Option[Double] = None

  /** Returns the max Y coordinate among this geometry points */
  final def yMax: Option[Double] = getCoordinate(_.y, _ > _)

  /** Returns the min Y coordinate among this geometry points */
  final def yMin: Option[Double] = getCoordinate(_.y, _ < _)

  /** Returns the Z coordinate of this geometry if it's a [[GisPoint]] */
  def z: Option[Double] = None

  /** Returns the max Z coordinate among this geometry points */
  final def zMax: Option[Double] = getCoordinate(_.z, _ > _)

  /** Returns the min Z coordinate among this geometry points */
  final def zMin: Option[Double] = getCoordinate(_.z, _ < _)

  final def m: Option[Double] = Option(geom.getUserData.asInstanceOf[Double])

  /** Dimension of this geometry */
  final def dimension: Int = geom.getDimension

  /** Exterior ring of this geometry if it's a [[GisPolygon]] */
  def exteriorRing: Option[GisGeometry] = None

  /** Number of rings of this geometry if it's a [[GisPolygon]] */
  final def numRings: Option[Int] = numInteriorRings.map(_ + 1)

  /** Number of interior rings of this geometry if it's a [[GisPolygon]] */
  def numInteriorRings: Option[Int] = None

  /** Returns the 0-based nth ring of this geometry if it's a [[GisPolygon]] */
  def interiorRing(n: Int): Option[GisGeometry] = None

  /** Returns a point inside this geometry if it's not empty */
  final def interiorPoint: Option[GisGeometry] = orElse(geom.getInteriorPoint)

  /** Returns the envelope of this geometry */
  final def envelope: GisGeometry = geom.getEnvelope

  /** Returns a geometry that represents all points whose distance from this geometry
    * is less than or equal to distance
    */
  final def buffer(distance: Double): GisGeometry = geom.buffer(distance)

  /** Returns the minimum convex geometry that encloses this one */
  final def convexHull: GisGeometry = geom.convexHull()

  /** Returns the difference between this and another geometry */
  final def difference(other: GisGeometry): GisGeometry = geom.difference(other.geom)

  /** Returns the symmetric difference between this and another geometry */
  final def symmetricDifference(other: GisGeometry): GisGeometry = geom.symDifference(other.geom)

  /** Returns the intersection between this and another geometry */
  final def intersection(other: GisGeometry): GisGeometry = geom.intersection(other.geom)

  /** Returns the union between this and another geometry */
  final def union(other: GisGeometry): GisGeometry = geom.union(other.geom)

  /** Returns the boundary of this geometry */
  def boundary: Option[GisGeometry] = {
    val b = geom.getBoundary
    if (b.isEmpty) None else Some(b)
  }

  /** Returns the dimension of the boundary of this geometry */
  final def boundaryDimension: Option[Int] = geom.getBoundaryDimension match {
    case Dimension.FALSE => None
    case x => Some(x)
  }

  /** Returns the coordinate dimension of this geometry */
  def coordinateDimension: Int = 0

  /** Returns the centroid of this geometry */
  final def centroid: Option[GisGeometry] = orElse(geom.getCentroid)

  /** Returns the start point of this geometry if it's a [[GisLineString]] */
  def startPoint: Option[GisGeometry] = None

  /** Returns the end point of this geometry if it's a [[GisLineString]] */
  def endPoint: Option[GisGeometry] = None

  /** Returns the length of this geometry if it's a [[GisLineString]] or a [[GisMultiLineString]] */
  def length: Option[Double] = None

  /** Returns the perimeter of this geometry if it's a [[GisPolygon]] or a [[GisMultiPolygon]] */
  def perimeter: Option[Double] = None

  /** Returns the area of this geometry if it's a [[GisPolygon]] or [[GisMultiPolygon]] */
  def area: Option[Double] = None

  /** Returns the distance between this geometry and another */
  final def distance(other: GisGeometry): Double = geom.distance(other.geom)

  /** Returns the number of sub geometries of this one */
  final def numGeometries: Int = geom.getNumGeometries

  /** Returns the 0-based nth sub geometries of this one */
  def subGeometry(n: Int): Option[GisGeometry] = None

  /** Tells if this geometry is empty */
  final def isEmpty: Boolean = geom.isEmpty

  /** Tells if this [[GisLineString]] or [[GisMultiLineString]] contains only rings */
  def isClosed: Boolean = false

  /** Tells if this is a [[GisGeometryCollection]] or one of its subclasses */
  def isCollection: Boolean = false

  /** Tells if this [[GisLineString]] is a ring, i.e. it's start and end point are coincident */
  def isRing: Boolean = false

  /** Tells if this geometry is simple */
  final def isSimple: Boolean = geom.isSimple

  /** Tells if this geometry contains another */
  final def contains(other: GisGeometry): Boolean = geom.contains(other.geom)

  /** Tells if this geometry crossed another */
  final def crosses(other: GisGeometry): Boolean = geom.crosses(other.geom)

  /** Tells if this geometry is disjoint from another */
  final def disjoint(other: GisGeometry): Boolean = geom.disjoint(other.geom)

  /** Tells if this geometry intersects another */
  final def intersects(other: GisGeometry): Boolean = geom.intersects(other.geom)

  /** Tells if this geometry overlaps another */
  final def overlaps(other: GisGeometry): Boolean = geom.overlaps(other.geom)

  /** Tells if this geometry touches another */
  final def touches(other: GisGeometry): Boolean = geom.touches(other.geom)

  /** Tells if this geometry is inside another */
  final def within(other: GisGeometry): Boolean = geom.within(other.geom)

  /** Tells if this geometry is spatially related to another wrt the given intersection matrix */
  final def relate(other: GisGeometry, matrix: String): Boolean =
    geom.relate(other.geom, matrix)

  /** Returns a copy of this geometry transformed to a different coordinate reference system */
  final def transformed(targetSRID: Int): GisGeometry = {
    import GisGeometry.ImplicitConversions.fromGeom

    // get coordinate reference systems
    val fromRS = CRS.decode(s"EPSG:$srid")
    val toRS = CRS.decode(s"EPSG:$targetSRID")

    // find transformation and bind it in a lambda
    val transformation = CRS.findMathTransform(fromRS, toRS, false)

    val rt = JTS.transform(geom, transformation)
    (rt, targetSRID)
  }

  /** Returns a simplified copy of this geometry */
  final def simplified(tolerance: Double): GisGeometry =
    TopologyPreservingSimplifier.simplify(geom, tolerance)

  /** Returns the `GeoJSON` representation of the enclosed [[Geometry]] */
  final def toGeoJson: String = {
    val writer = new StringWriter()
    new GeometryJSON().write(geom, writer)
    writer.toString
  }

  /** Returns the `WKB` representation of the enclosed [[Geometry]] */
  final def toBinary: Array[Byte] =
    new WKBWriter().write(geom)

  /** Returns the `WKT` representation of the enclosed [[Geometry]] */
  final override def toString: String =
    new WKTWriter().write(geom)

  /** Tells if this geometry is equal to another */
  final override def equals(other: Any) = other match {
    case g: GisGeometry => geom.equals(g.geom)
    case _ => false
  }
}

/** Wrapper for GeoTools [[Point]] */
class GisPoint(val point: Point, _srid: Option[Int] = None)
  extends GisGeometry(point, _srid) {

  private lazy val coord = point.getCoordinates.head

  override def x: Option[Double] = Some(coord.x)

  override def y: Option[Double] = Some(coord.y)

  override def z: Option[Double] = Some(coord.z)

  override def coordinateDimension: Int = point.getCoordinateSequence.getDimension
}

/** Wrapper for GeoTools [[LineString]] */
class GisLineString(val line: LineString, _srid: Option[Int] = None)
  extends GisGeometry(line, _srid) {

  import GisGeometry.ImplicitConversions.fromGeom

  override def isClosed: Boolean = line.isClosed

  override def isRing: Boolean = line.isRing

  override def startPoint: Option[GisGeometry] = orElse(line.getStartPoint)

  override def endPoint: Option[GisGeometry] = orElse(line.getEndPoint)

  override def length: Option[Double] = Some(line.getLength)

  override def coordinateDimension: Int = line.getCoordinateSequence.getDimension
}

class GisPolygon(val poly: Polygon, _srid: Option[Int] = None)
  extends GisGeometry(poly, _srid) {

  import GisGeometry.ImplicitConversions.fromGeom

  /** Returns every ring in this polygon */
  def rings =
    (poly.getExteriorRing +:
      (0 until poly.getNumInteriorRing).
      map(poly.getInteriorRingN)).
      map(GisLineString.apply(_))

  override def exteriorRing: Option[GisGeometry] = Option(poly.getExteriorRing)

  override def numInteriorRings: Option[Int] = Some(poly.getNumInteriorRing)

  override def interiorRing(n: Int): Option[GisGeometry] = Option(poly.getInteriorRingN(n))

  override def perimeter: Option[Double] = Some(poly.getLength)

  override def area: Option[Double] = Some(poly.getArea)

  override def coordinateDimension: Int =
    rings.map(_.coordinateDimension).max
}

/** Wrapper for GeoTools [[GeometryCollection]] */
class GisGeometryCollection(val collection: GeometryCollection, _srid: Option[Int] = None)
  extends GisGeometry(collection, _srid) {

  import GisGeometry.ImplicitConversions.fromGeom

  /** Returns the sub geometries in this collection, optionally cast to a specific type */
  def subGeometries[T <: GisGeometry] =
    (0 until numGeometries).
      map(collection.getGeometryN).
      map(GisGeometry.apply(_)).
      map(_.asInstanceOf[T])

  override def boundary: Option[GisGeometry] = None

  override def subGeometry(n: Int): Option[GisGeometry] =
    if (n < numGeometries) Option(collection.getGeometryN(n))
    else None

  override def isCollection: Boolean = true

  override def coordinateDimension: Int =
    subGeometries[GisGeometry].map(_.coordinateDimension).max
}

/** Wrapper for GeoTools [[MultiPoint]] */
class GisMultiPoint(val multiPoint: MultiPoint, _srid: Option[Int] = None)
  extends GisGeometryCollection(multiPoint, _srid) {

}

/** Wrapper for GeoTools [[MultiLineString]] */
class GisMultiLineString(val multiLine: MultiLineString, _srid: Option[Int] = None)
  extends GisGeometryCollection(multiLine, _srid) {

  override def isClosed: Boolean = multiLine.isClosed

  override def length: Option[Double] = Some(multiLine.getLength)
}

/** Wrapper for GeoTools [[MultiPolygon]] */
class GisMultiPolygon(val multiPoly: MultiPolygon, _srid: Option[Int] = None)
  extends GisGeometryCollection(multiPoly, _srid) {

  override def area: Option[Double] = Some(multiPoly.getArea)

  override def perimeter: Option[Double] = Some(multiPoly.getLength)
}

/** Factory methods and implicit conversions */
object GisGeometry {

  /** Returns the right instance of a [[GisGeometry]] subclass given an input GeoTools geometry */
  def apply(gt: Geom, srid: Option[Int] = None): GisGeometry = gt match {
    case v: Point => GisPoint(v, srid)
    case v: MultiPoint => GisMultiPoint(v, srid)
    case v: LineString => GisLineString(v, srid)
    case v: MultiLineString => GisMultiLineString(v, srid)
    case v: Polygon => GisPolygon(v, srid)
    case v: MultiPolygon => GisMultiPolygon(v, srid)
    case c: GeometryCollection => GisGeometryCollection(c, srid)
    case _ => throw new IllegalArgumentException("Unknown geometry type: " + gt.getGeometryType)
  }

  /** Extracts sub geometries of a [[GisGeometryCollection]] */
  def unapply(c: GisGeometryCollection): Option[Seq[GisGeometry]] =
    c.orElse(c.subGeometries)

  private[gis]
  object ImplicitConversions {

    /** Extracts the [[GisGeometry]] contained in a [[Geometry]] */
    implicit def fromGeometry(g: Geometry): GisGeometry =
      g.impl

    /** Builds a [[GisGeometry]] from a GeoTools one paired with an SRID */
    implicit def fromGeom(geom: (Geom, Int)): GisGeometry =
      GisGeometry.apply(geom._1, Some(geom._2))

    /** Builds a [[GisGeometry]] from a GeoTools one */
    implicit def fromGeom(geom: Geom): GisGeometry =
      GisGeometry.apply(geom)
  }
}

/** Factory method and extractors for [[GisPoint]] */
object GisPoint {

  def apply(p: Point, srid: Option[Int] = None): GisPoint =
    new GisPoint(p, srid)

  def unapply(g: Geometry): Option[GisPoint] = g.impl match {
    case p: GisPoint => Some(p)
    case _ => None
  }

  def unapply(mp: GisMultiPoint): Option[Seq[GisPoint]] =
    mp.orElse(mp.subGeometries[GisPoint])
}

/** Factory method and extractors for [[GisLineString]] */
object GisLineString {

  def apply(p: LineString, srid: Option[Int] = None): GisLineString =
    new GisLineString(p, srid)

  def unapply(g: Geometry): Option[GisLineString] = g.impl match {
    case p: GisLineString => Some(p)
    case _ => None
  }

  def unapply(ml: GisMultiLineString): Option[Seq[GisLineString]] =
    ml.orElse(ml.subGeometries[GisLineString])
}

/** Factory method and extractors for [[GisPolygon]] */
object GisPolygon {

  def apply(p: Polygon, srid: Option[Int] = None): GisPolygon =
    new GisPolygon(p, srid)

  def unapply(g: Geometry): Option[GisPolygon] = g.impl match {
    case p: GisPolygon => Some(p)
    case _ => None
  }

  def unapply(mp: GisMultiPolygon): Option[Seq[GisPolygon]] =
    mp.orElse(mp.subGeometries[GisPolygon])
}

/** Factory method and extractors for [[GisGeometryCollection]] */
object GisGeometryCollection {

  def apply(p: GeometryCollection, srid: Option[Int] = None): GisGeometryCollection =
    new GisGeometryCollection(p, srid)

  def unapply(g: Geometry): Option[GisGeometryCollection] = g.impl match {
    case p: GisGeometryCollection => Some(p)
    case _ => None
  }
}

/** Factory method and extractors for [[GisMultiPoint]] */
object GisMultiPoint {

  def apply(p: MultiPoint, srid: Option[Int] = None): GisMultiPoint =
    new GisMultiPoint(p, srid)

  def unapply(g: Geometry): Option[GisMultiPoint] = g.impl match {
    case p: GisMultiPoint => Some(p)
    case _ => None
  }

}


/** Factory method and extractors for [[GisMultiLineString]] */
object GisMultiLineString {

  def apply(p: MultiLineString, srid: Option[Int] = None): GisMultiLineString =
    new GisMultiLineString(p, srid)

  def unapply(g: Geometry): Option[GisMultiLineString] = g.impl match {
    case p: GisMultiLineString => Some(p)
    case _ => None
  }
}

/** Factory method and extractors for [[GisMultiPolygon]] */
object GisMultiPolygon {

  def apply(p: MultiPolygon, srid: Option[Int] = None): GisMultiPolygon =
    new GisMultiPolygon(p, srid)

  def unapply(g: Geometry): Option[GisMultiPolygon] = g.impl match {
    case p: GisMultiPolygon => Some(p)
    case _ => None
  }
}

/** Extractors to get [[Coordinate]] (or sequences of) from [[GisGeometry]] instances */
object Coordinate {

  private def toCoordinate(c: Coord): Coordinate =
    (c.x, c.y)

  def unapply(p: GisPoint): Option[Coordinate] =
    p.orElse((p.x.get, p.y.get))

  def unapply(l: GisLineString): Option[Seq[Coordinate]] =
    l.orElse(l.allCoordinates.map(toCoordinate))

  def unapply(p: GisPolygon): Option[Seq[Seq[Coordinate]]] =
    p.orElse(p.rings.map(unapply).flatten)

  def unapply(mp: GisMultiPoint): Option[Seq[Coordinate]] =
    mp.orElse(mp.allCoordinates.map(toCoordinate))

  def unapply(ml: GisMultiLineString): Option[Seq[Seq[Coordinate]]] =
    ml.orElse(ml.subGeometries[GisLineString].map(unapply).flatten)

  def unapply(mp: GisMultiPolygon): Option[Seq[Seq[Seq[Coordinate]]]] =
    mp.orElse(mp.subGeometries[GisPolygon].map(unapply).flatten)

}