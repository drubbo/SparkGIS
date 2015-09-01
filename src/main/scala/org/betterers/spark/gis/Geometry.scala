package org.betterers.spark.gis

import java.nio.ByteBuffer

import com.esri.core.geometry._
import com.esri.core.geometry.ogc.{OGCConcreteGeometryCollection, OGCGeometry, OGCPoint}
import org.apache.log4j.Logger
import org.apache.spark.sql.types.SQLUserDefinedType

import scala.collection.JavaConversions

/**
 * Class wrapping an [[ESRIGeometry]] representing values of [[GeometryType]]
 *
 * @author Ubik <emiliano.leporati@gmail.com>
 */
@SQLUserDefinedType(udt = classOf[GeometryType])
class Geometry(val srid: Int, private[gis] val geometries: Seq[ESRIGeometry]) extends Serializable {

  /**
   * Builds a [[Geometry]] from an [[ESRIGeometry]]
   * @param srid
   * @param geom
   */
  def this(srid: Int, geom: ESRIGeometry) = {
    this(srid, Seq(geom))
  }

  /**
   * Builds a [[Geometry]] from an [[ESRIGeometry]] with default SRID
   * @param geom
   */
  def this(geom: ESRIGeometry) = {
    this(Geometry.WGS84, geom)
  }

  /**
   * [[OGCGeometry]] built from the enclosed [[ESRIGeometry]]
   */
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

  /**
   * Collection of points for each enclosed geometry
   */
  @transient
  private lazy val geomPoints: Seq[Seq[Point]] =
    geometries.map(Utils.getPoints)

  /**
   * Collection of every point in every enclosed geometry
   */
  @transient
  private lazy val allPoints: Seq[Point] =
    geomPoints.flatten


  /**
   * @note Geometry collections not supported
   * @return centroid of the enclosed geometry
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
    def pathCentroid[T <: MultiPath](geom: T, pathIndex: Int, weight: Double): (Point, Double) = {
      val points = Seq.range(geom.getPathStart(pathIndex), geom.getPathEnd(pathIndex)).map(geom.getPoint)
      val x = Utils.avgCoordinate(_.getX, points) * weight
      val y = Utils.avgCoordinate(_.getY, points) * weight
      println(s"Centroid of $points weight $weight")
      (new Point(x, y), weight)
    }
    // centroid of a single shape
    def singleCentroid: ESRIGeometry => Point = {
      case (g: Point) =>
        g
      case (_: Segment | _: MultiPoint) =>
        centroid(geomPoints.head)
      case g: Polyline =>
        weightedCentroid(Seq.range(0, g.getPathCount).map(p => {
          pathCentroid(g, p, g.calculatePathLength2D(p))
        }))
      case g: Polygon =>
        weightedCentroid(Seq.range(0, g.getPathCount).map(p => {
          pathCentroid(g, p, g.calculateRingArea2D(p))
        }))
    }

    geometries match {
      case g +: Nil => Some(singleCentroid(g))
      case _ => None
    }
  }

  /**
   * @return Number of rings if enclosed [[ESRIGeometry]] is a [[Polygon]]
   */
  def numberOfRings: Option[Int] = geometries match {
    case (p: Polygon) +: Nil => Some(p.getPathCount)
    case _ => None
  }

  /**
   * @return Number of sub-geometries
   */
  def numberOfGeometries =
    geometries match {
      case (_: Point | _: Segment) +: Nil => 1
      case (m: MultiVertexGeometry) +: Nil => m.getPointCount
      case _ => geometries.size
    }

  /**
   * @param coord Coordinate extractor from point
   * @return maximum coordinate
   *         None for empty geometry
   */
  private[gis]
  def maxCoordinate(coord: Point => Double): Option[Double] =
    getCoordinateBoundary(coord, _ > _)

  /**
   * @param coord Coordinate extractor from point
   * @return minimum coordinate
   *         None for empty geometry
   */
  private[gis]
  def minCoordinate(coord: Point => Double): Option[Double] =
    getCoordinateBoundary(coord, _ < _)

  /**
   * Returns a coordinate of the enclosed geometries satisfying some predicate
   * @param pred Predicate to satisfy (be the greatest, least, etc)
   * @param coord Function to extract the required coordinate from a point
   * @return
   */
  private def getCoordinateBoundary(coord: (Point => Double), pred: ((Double, Double) => Boolean)) = {
    if (geometries.isEmpty) None
    else Some(allPoints.map(coord).reduceLeft((a, b) => if (pred(a, b)) a else b))
  }

  /**
   * @return JSON representation of the enclosed [[ESRIGeometry]]
   */
  def toJson: String =
    ogc.asJson

  /**
   * @return GeoJSON representation of the enclosed [[ESRIGeometry]]
   */
  def toGeoJson: String =
    ogc.asGeoJson

  /**
   * @return WKB representation of the enclosed [[ESRIGeometry]]
   */
  def toBinary: Array[Byte] =
    ogc.asBinary.array

  /**
   * @return WKT representation of the enclosed [[ESRIGeometry]]
   */
  override def toString: String =
    ogc.asText

  override def equals(other: Any) = other match {
    case g: Geometry => this.toString == g.toString
    case _ => false
  }
}

/**
 * Factory methods for [[Geometry]]
 */
object Geometry {
  private val LOG = Logger.getLogger(classOf[Geometry])

  /**
   * Default spatial reference
   */
  private val WGS84 = 4326

  /**
   * Builds a [[Geometry]] from and ESRI [[OGCGeometry]]
   * @param geom
   */
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

  /**
   * @param json
   * @return A [[Geometry]] built from a json REST string
   */
  def fromJson(json: String): Geometry =
    apply(OGCGeometry.fromJson(json))

  /**
   * @param geoJson
   * @return A [[Geometry]] built from a geoJson string
   */
  def fromGeoJson(geoJson: String): Geometry =
    apply(OGCGeometry.fromGeoJson(geoJson))

  /**
   * @param wkb
   * @return A [[Geometry]] built from a WKB
   */
  def fromBinary(wkb: ByteBuffer): Geometry =
    apply(OGCGeometry.fromBinary(wkb))

  /**
   * @param wkb
   * @return A [[Geometry]] built from a WKB
   */
  def fromBinary(wkb: Array[Byte]): Geometry =
    fromBinary(ByteBuffer.wrap(wkb))

  /**
   * @param wkt
   * @return A [[Geometry]] built from a WKT string
   */
  def fromString(wkt: String): Geometry =
    apply(OGCGeometry.fromText(wkt))

  /**
   * @param srid
   * @param xy
   * @return A [[Geometry]] enclosing a [[Point]]
   */
  def point(srid: Int, xy: (Double, Double)): Geometry =
    new Geometry(srid, GeometryBuilder.mkPoint(xy))

  /**
   * @param xy
   * @return A [[Geometry]] enclosing a [[Point]] with default SRID
   */
  def point(xy: (Double, Double)): Geometry =
    point(WGS84, xy)

  /**
   * @param srid
   * @param points
   * @return A [[Geometry]] enclosing a [[MultiPoint]]
   */
  def multiPoint(srid: Int, points: (Double, Double)*): Geometry =
    new Geometry(srid, GeometryBuilder.mkMultiPoint(points))

  /**
   * @param points
   * @return A [[Geometry]] enclosing a [[MultiPoint]] with default SRID
   */
  def multiPoint(points: (Double, Double)*): Geometry =
    multiPoint(WGS84, points: _*)

  /**
   * @param srid
   * @param points
   * @return A [[Geometry]] enclosing a [[Polyline]] with a single path
   */
  def line(srid: Int, points: (Double, Double)*): Geometry =
    new Geometry(srid, GeometryBuilder.mkLine(points))

  /**
   * @param points
   * @return A [[Geometry]] enclosing a [[Polyline]] with a single path and default SRID
   */
  def line(points: (Double, Double)*): Geometry =
    line(WGS84, points: _*)

  /**
   * @param srid
   * @param lines
   * @return A [[Geometry]] enclosing a [[Polyline]] with multiple paths
   */
  def multiLine(srid: Int, lines: Seq[(Double, Double)]*): Geometry =
    new Geometry(srid, GeometryBuilder.mkMultiLine(lines))

  /**
   * @param lines
   * @return A [[Geometry]] enclosing a [[Polyline]] with multiple paths and default SRID
   */
  def multiLine(lines: Seq[(Double, Double)]*): Geometry =
    multiLine(WGS84, lines: _*)

  /**
   * @param srid
   * @param points
   * @return A [[Geometry]] enclosing a [[Polygon]] with a single ring
   */
  def polygon(srid: Int, points: (Double, Double)*): Geometry =
    new Geometry(srid, GeometryBuilder.mkPolygon(points))

  /**
   * @param points
   * @return A [[Geometry]] enclosing a [[Polygon]] with a single ring and default SRID
   */
  def polygon(points: (Double, Double)*): Geometry =
    polygon(WGS84, points: _*)

  /**
   * @param srid
   * @param lines
   * @return A [[Geometry]] enclosing a [[Polygon]] with multiple rings
   */
  def multiPolygon(srid: Int, lines: Seq[(Double, Double)]*): Geometry =
    new Geometry(srid, GeometryBuilder.mkMultiPolygon(lines))

  /**
   * @param lines
   * @return A [[Geometry]] enclosing a [[Polygon]] with multiple rings and default SRID
   */
  def multiPolygon(lines: Seq[(Double, Double)]*): Geometry =
    multiPolygon(WGS84, lines: _*)

  /**
   * @param srid
   * @param geometries
   * @return A [[Geometry]] enclosing a geometry collection
   */
  def collection(srid: Int, geometries: ESRIGeometry*): Geometry = {
    val sr = SpatialReference.create(srid)
    val ogcGeoms = geometries.map(OGCGeometry.createFromEsriGeometry(_, sr))
    val geoColl = new OGCConcreteGeometryCollection(JavaConversions.seqAsJavaList(ogcGeoms), sr)
    apply(geoColl)
  }

  /**
   * @param geometries
   * @return A [[Geometry]] enclosing a geometry collection with default SRID
   */
  def collection(geometries: ESRIGeometry*): Geometry =
    collection(WGS84, geometries: _*)

  /**
   * @param geometries
   * @return A [[Geometry]] enclosing a geometry collection
   */
  def aggregate(srid: Int, geometries: Geometry*): Geometry = {
    val geoms = geometries.flatMap(_.geometries)
    collection(srid, geoms: _*)
  }

  /**
   * @param geometries
   * @return A [[Geometry]] enclosing a geometry collection with default SRID
   */
  def aggregate(geometries: Geometry*): Geometry =
    aggregate(WGS84, geometries: _*)
}
