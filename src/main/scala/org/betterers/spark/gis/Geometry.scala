package org.betterers.spark.gis

import com.esri.core.geometry.{GeometryCursor, SimpleGeometryCursor, SpatialReference}
import com.esri.core.geometry.{Point, MultiPoint, Segment, Line, Polyline, Polygon, MultiVertexGeometry}
import com.esri.core.geometry.ogc.{OGCConcreteGeometryCollection, OGCGeometry, OGCPoint}
import org.apache.log4j.Logger
import org.apache.spark.sql.types.SQLUserDefinedType

import scala.collection.JavaConversions

/**
 * Class wrapping an [[ESRIGeometry]] representing values of [[GeometryType]]
 *
 * @author drubbo <ubik@gamezoo.it>
 */
@SQLUserDefinedType(udt = classOf[GeometryType])
class Geometry(val srid: Int, val geom: Seq[ESRIGeometry]) extends Serializable {

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
   * Builds a [[Geometry]] from and ESRI [[OGCGeometry]]
   * @param geom
   */
  def this(geom: OGCGeometry) = {
    this(geom.SRID, {
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
   * [[OGCGeometry]] built from the enclosed [[ESRIGeometry]]
   */
  @transient
  lazy val ogc: OGCGeometry = {
    val sr = SpatialReference.create(srid)
    if (geom.isEmpty) new OGCPoint(new Point(), sr)
    else {
      val cursor = new SimpleGeometryCursor(geom.toArray)
      OGCGeometry.createFromEsriCursor(cursor, sr)
    }
  }

  /**
   * @return Number of rings if enclosed [[ESRIGeometry]] is a [[Polygon]]
   */
  def numberOfRings: Option[Int] = geom match {
    case (p: Polygon) +: Nil => Some(p.getPathCount)
    case _ => None
  }

  /**
   * @return Number of sub-geometries
   */
  def numberOfGeometries =
    geom match {
      case (_: Point) +: Nil => 1
      case (_: Line) +: Nil => 1
      case (m: MultiVertexGeometry) +: Nil => m.getPointCount
      case _ => geom.size
    }

  /**
   * @param coord Coordinate extractor from point
   * @return maximum coordinate
   *         None for empty geometry
   */
  def maxCoordinate(coord: Point => Double): Option[Double] =
    getCoordinateBoundary(coord, _ > _)

  /**
   * @param coord Coordinate extractor from point
   * @return minimum coordinate
   *         None for empty geometry
   */
  def minCoordinate(coord: Point => Double): Option[Double] =
    getCoordinateBoundary(coord, _ < _)

  /**
   * Returns a coordinate of the enclosed geometries satisfying some predicate
   * @param pred Predicate to satisfy (be the greatest, least, etc)
   * @param coord Function to extract the required coordinate from a point
   * @return
   */
  private def getCoordinateBoundary(coord: (Point => Double), pred: ((Double, Double) => Boolean)) = {
    def search: Seq[ESRIGeometry] => Double = {
      case (x: Point) +: Nil =>
        // first value
        coord(x)
      case (x: Point) +: tail =>
        // get candidate from tail evaluation and return the best
        val candidate = search(tail)
        val opponent = coord(x)
        if (pred(opponent, candidate)) opponent else candidate
      case (x: Segment) +: tail =>
        // expand the segment in two points and search
        search(new Point(x.getStartX, x.getStartY) +: new Point(x.getEndX, x.getEndY) +: tail)
      case (x: MultiVertexGeometry) +: tail =>
        // expand each vertex and search
        def expand: Int => Seq[ESRIGeometry] = {
          case i if i < x.getPointCount =>
            x.getPoint(i) +: expand(i + 1)
          case _ =>
            tail
        }
        search(expand(0))
    }
    if (geom.isEmpty) None else Some(search(geom))
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
   * @param json
   * @return A [[Geometry]] built from a json string
   */
  def fromJson(json: String): Geometry =
    new Geometry(OGCGeometry.fromJson(json))

  /**
   * @param geoJson
   * @return A [[Geometry]] built from a geoJson string
   */
  def fromGeoJson(geoJson: String): Geometry =
    new Geometry(OGCGeometry.fromGeoJson(geoJson))

  /**
   * @param wkt
   * @return A [[Geometry]] built from a WKT string
   */
  def fromString(wkt: String): Geometry =
    new Geometry(OGCGeometry.fromText(wkt))

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
    new Geometry(geoColl)
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
    val geoms = geometries.flatMap(_.geom)
    collection(srid, geoms: _*)
  }

  /**
   * @param geometries
   * @return A [[Geometry]] enclosing a geometry collection with default SRID
   */
  def aggregate(geometries: Geometry*): Geometry =
    aggregate(WGS84, geometries: _*)
}
