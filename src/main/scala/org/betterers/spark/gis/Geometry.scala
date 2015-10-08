package org.betterers.spark.gis

import java.nio.ByteBuffer

import com.esri.core.geometry._
import com.esri.core.geometry.ogc.{OGCConcreteGeometryCollection, OGCGeometry, OGCPoint}
import org.apache.spark.sql.types.SQLUserDefinedType

import scala.collection.JavaConversions

/** Class wrapping an [[ESRIGeometry]] representing values of [[GeometryType]]
  *
  * @constructor Builds a new geometry given an SRID and a set of [[ESRIGeometry]]
  * @param srid Spatial Reference ID for this geometry
  * @author Ubik <emiliano.leporati@gmail.com>
  */
@SQLUserDefinedType(udt = classOf[GeometryType])
class Geometry(val srid: Int, private[gis] val geometries: Seq[ESRIGeometry]) extends Serializable {

  /** Builds a geometry given an SRID and a single [[ESRIGeometry]] */
  def this(srid: Int, geom: ESRIGeometry) =
    this(srid, Seq(geom))

  /** Builds a geometry with default SRID given a single [[ESRIGeometry]] */
  def this(geom: ESRIGeometry) =
    this(Geometry.WGS84, geom)

  /** [[OGCGeometry]] built from the enclosed [[ESRIGeometry]] */
  @transient
  private[gis]
  lazy val ogc: OGCGeometry = {
    val sr = SpatialReference.create(srid)
    if (geometries.isEmpty) new OGCPoint(new Point(), sr)
    else {
      val cursor = new SimpleGeometryCursor(geometries.toArray)
      OGCGeometry.createFromEsriCursor(cursor, sr)
    }
  }

  /** Collection of points for each enclosed geometry */
  @transient
  private lazy val geomPoints: Seq[Seq[Point]] =
    geometries.map(Utils.getPoints)

  /** Collection of every point in every enclosed geometry */
  @transient
  private lazy val allPoints: Seq[Point] =
    geomPoints.flatten

  /** Transforms this geometry coordinates to a different coordinate reference system */
  def transformTo(targetSRID: Int): Geometry =
    Transform(this, targetSRID)

  /** Returns the centroid of the enclosed geometry
    * @note Geometry collections not supported
    */
  def centroid: Option[Point] = {
    // average point of a sequence
    def centroid(points: Seq[Point]): Point = {
      val x = Utils.avgCoordinate(_.getX, points)
      val y = Utils.avgCoordinate(_.getY, points)
      new Point(x, y)
    }
    // average point of a weighted sequence
    def weightedCentroid(points: Seq[(Point, Double)]): Point = {
      val pw = points.unzip
      val x = Utils.avgWeightCoordinate(_.getX, pw)
      val y = Utils.avgWeightCoordinate(_.getY, pw)
      new Point(x, y)
    }
    // centroid weighted by length / area
    def pathCentroid(geom: MultiPath, pathIndex: Int, weight: Double): (Point, Double) = {
      val points = Utils.getPoints(geom, pathIndex)
      val x = Utils.avgCoordinate(_.getX, points) * weight
      val y = Utils.avgCoordinate(_.getY, points) * weight
      (new Point(x, y), weight)
    }
    // centroid of a single shape
    def singleCentroid: ESRIGeometry => Point = {
      case g: Point =>
        g
      case (_: Segment | _: MultiPoint) =>
        centroid(geomPoints.head)
      case g: Polyline =>
        weightedCentroid(Range(0, g.getPathCount).map(p => {
          pathCentroid(g, p, g.calculatePathLength2D(p))
        }))
      case g: Polygon =>
        weightedCentroid(Range(0, g.getPathCount).map(p => {
          pathCentroid(g, p, g.calculateRingArea2D(p))
        }))
    }

    geometries match {
      case g +: Nil => Some(singleCentroid(g))
      case _ => None
    }
  }

  /** Returns the number of rings if enclosed [[ESRIGeometry]] is a [[Polygon]] */
  def numberOfRings: Option[Int] = geometries match {
    case (p: Polygon) +: Nil => Some(p.getPathCount)
    case _ => None
  }

  /** Returns the number of sub-geometries */
  def numberOfGeometries =
    geometries match {
      case (_: Point | _: Segment) +: Nil => 1
      case (m: MultiVertexGeometry) +: Nil => m.getPointCount
      case _ => geometries.size
    }

  /** Returns the maximum coordinate of this geometry given a coordinate extractor, or None for empty geometry */
  private[gis]
  def maxCoordinate(coord: Point => Double): Option[Double] =
    getCoordinateBoundary(coord, _ > _)

  /** Returns the minimum coordinate of this geometry given a coordinate extractor, or None for empty geometry */
  private[gis]
  def minCoordinate(coord: Point => Double): Option[Double] =
    getCoordinateBoundary(coord, _ < _)

  /** Returns a coordinate of the enclosed geometries satisfying some predicate
    *
    * @param pred Predicate to satisfy (be the greatest, least, etc)
    * @param coord Function to extract the required coordinate from a point
    */
  private def getCoordinateBoundary(coord: Point => Double, pred: (Double, Double) => Boolean) = {
    if (geometries.isEmpty) None
    else Some(allPoints.map(coord).reduceLeft((a, b) => if (pred(a, b)) a else b))
  }

  /** Returns the `JSON` representation of the enclosed [[ESRIGeometry]] */
  def toJson: String =
    ogc.asJson

  /** Returns the `GeoJSON` representation of the enclosed [[ESRIGeometry]] */
  def toGeoJson: String =
    ogc.asGeoJson

  /** Returns the `WKB` representation of the enclosed [[ESRIGeometry]] */
  def toBinary: Array[Byte] =
    ogc.asBinary.array

  /** Returns the `WKT` representation of the enclosed [[ESRIGeometry]] */
  override def toString: String =
    ogc.asText

  /** Returns true if the other object is a geometry with the same `WKT` representation of this one */
  override def equals(other: Any) = other match {
    case g: Geometry => this.toString == g.toString
    case _ => false
  }
}

/** Factory methods for [[Geometry]] */
object Geometry {

  /** Type for coordinates pair */
  type Coordinates = (Double, Double)

  /** Default spatial reference */
  val WGS84 = 4326

  /** Builds a geometry from and ESRI [[OGCGeometry]] */
  private[gis]
  def apply(geom: OGCGeometry) = {
    new Geometry(geom.SRID, {
      def curToSeq(cur: GeometryCursor): Seq[ESRIGeometry] = {
        cur.next() match {
          case null => Nil
          case g => g +: curToSeq(cur)
        }
      }
      curToSeq(geom.getEsriGeometryCursor)
    })
  }

  import GeometryBuilder._

  /** Builds a geometry from a `JSON REST` string */
  def fromJson(json: String): Geometry =
    apply(OGCGeometry.fromJson(json))

  /** Builds a geometry from a `GeoJSON` string */
  def fromGeoJson(geoJson: String): Geometry =
    apply(OGCGeometry.fromGeoJson(geoJson))

  /** Builds a geometry from a `WKB` */
  def fromBinary(wkb: ByteBuffer): Geometry =
    apply(OGCGeometry.fromBinary(wkb))

  /** Builds a geometry from a `WKB` */
  def fromBinary(wkb: Array[Byte]): Geometry =
    fromBinary(ByteBuffer.wrap(wkb))

  /** Builds a geometry from a `WKT` string */
  def fromString(wkt: String): Geometry =
    apply(OGCGeometry.fromText(wkt))

  /** Builds a geometry enclosing a [[Point]] */
  def point(srid: Int, xy: Coordinates): Geometry =
    new Geometry(srid, mkPoint(xy))

  /** Builds a geometry enclosing a [[Point]] with default SRID */
  def point(xy: Coordinates): Geometry =
    point(WGS84, xy)

  /** Builds a geometry enclosing a [[Point]] */
  def point(srid: Int, x: Double, y: Double): Geometry =
    new Geometry(srid, mkPoint((x, y)))

  /** Builds a geometry enclosing a [[Point]] with default SRID */
  def point(x: Double, y: Double): Geometry =
    point(WGS84, x, y)

  /** Builds a geometry enclosing a [[MultiPoint]] */
  def multiPoint(srid: Int, points: Coordinates*): Geometry =
    new Geometry(srid, mkMultiPoint(points))

  /** Builds a geometry enclosing a [[MultiPoint]] with default SRID */
  def multiPoint(points: Coordinates*): Geometry =
    multiPoint(WGS84, points: _*)

  /** Builds a geometry enclosing a [[Polyline]] with a single path */
  def line(srid: Int, points: Coordinates*): Geometry =
    new Geometry(srid, mkLine(points))

  /** Builds a geometry enclosing a [[Polyline]] with a single path and default SRID */
  def line(points: Coordinates*): Geometry =
    line(WGS84, points: _*)

  /** Builds a geometry enclosing a [[Polyline]] with multiple paths */
  def multiLine(srid: Int, lines: Seq[Coordinates]*): Geometry =
    new Geometry(srid, mkMultiLine(lines))

  /** Builds a geometry enclosing a [[Polyline]] with multiple paths and default SRID */
  def multiLine(lines: Seq[Coordinates]*): Geometry =
    multiLine(WGS84, lines: _*)

  /** Builds a geometry enclosing a [[Polygon]] with a single ring */
  def polygon(srid: Int, points: Coordinates*): Geometry =
    new Geometry(srid, mkPolygon(points))

  /** Builds a geometry enclosing a [[Polygon]] with a single ring and default SRID */
  def polygon(points: Coordinates*): Geometry =
    polygon(WGS84, points: _*)

  /** Builds a geometry enclosing a [[Polygon]] with multiple rings */
  def multiPolygon(srid: Int, lines: Seq[Coordinates]*): Geometry =
    new Geometry(srid, mkMultiPolygon(lines))

  /** Builds a geometry enclosing a [[Polygon]] with multiple rings and default SRID */
  def multiPolygon(lines: Seq[Coordinates]*): Geometry =
    multiPolygon(WGS84, lines: _*)

  /** Builds a geometry enclosing a geometry collection */
  def collection(srid: Int, geometries: ESRIGeometry*): Geometry = {
    val sr = SpatialReference.create(srid)
    val ogcGeoms = geometries.map(OGCGeometry.createFromEsriGeometry(_, sr))
    val geoColl = new OGCConcreteGeometryCollection(JavaConversions.seqAsJavaList(ogcGeoms), sr)
    apply(geoColl)
  }

  /** Builds a geometry enclosing a geometry collection with default SRID */
  def collection(geometries: ESRIGeometry*): Geometry =
    collection(WGS84, geometries: _*)

  /** Builds a geometry enclosing a geometry collection
    * combining every [[ESRIGeometry]] enclosed in input arguments.
    *
    * Input geometries will be transformed to given coordinate system.
    */
  def aggregate(srid: Int, geometries: Geometry*): Geometry = {
    val geoms = geometries.map(Transform(_, srid)).flatMap(_.geometries)
    collection(srid, geoms: _*)
  }

  /** Builds a geometry enclosing a geometry collection
    * combining every [[ESRIGeometry]] enclosed in input arguments.
    *
    * Input geometries must share the same SRID; if no geometry is
    * provided, an empty geometry with default SRID is returned.
    */
  def aggregate(geometries: Geometry*): Geometry = {
    if (geometries.isEmpty)
      aggregate(WGS84)
    else {
      val srids = geometries.map(_.srid)
      if (srids.distinct.length > 1)
        throw new IllegalArgumentException("Geometries must be in the same coordinate reference system")
      else {
        aggregate(srids.head, geometries: _*)
      }
    }
  }
}
