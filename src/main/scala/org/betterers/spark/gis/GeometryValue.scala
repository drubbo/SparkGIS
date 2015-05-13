package org.betterers.spark.gis

import com.esri.core.geometry._
import com.esri.core.geometry.ogc.{OGCConcreteGeometryCollection, OGCGeometry}
import org.apache.log4j.Logger
import org.apache.spark.sql.types.SQLUserDefinedType

import scala.collection.JavaConversions

/**
 * Class wrapping an ESRI [[Geometry]] representing values of [[GeometryType]]
 *
 * @author drubbo <ubik@gamezoo.it>
 */
@SQLUserDefinedType(udt = classOf[GeometryType])
class GeometryValue(val srid: Int, val geom: Seq[Geometry]) extends Serializable {

  /**
   * Builds a [[GeometryValue]] from an ESRI [[Geometry]]
   * @param srid
   * @param geom
   */
  def this(srid: Int, geom: Geometry) = {
    this(srid, Seq(geom))
  }

  /**
   * Builds a [[GeometryValue]] from an ESRI [[Geometry]] with default SRID
   * @param geom
   */
  def this(geom: Geometry) = {
    this(GeometryValue.WGS84, geom)
  }

  /**
   * Builds a [[GeometryValue]] from and ESRI [[OGCGeometry]]
   * @param geom
   */
  def this(geom: OGCGeometry) = {
    this(geom.SRID, {
      def curToSeq(cur: GeometryCursor): Seq[Geometry] = {
        cur.next() match {
          case null => Nil
          case g => g +: curToSeq(cur)
        }
      }
      curToSeq(geom.getEsriGeometryCursor)
    })
  }

  /**
   * [[OGCGeometry]] built from the enclosed [[Geometry]]
   */
  @transient
  lazy val ogc = {
    if (geom == null) null
    else {
      val cursor = new SimpleGeometryCursor(geom.toArray)
      OGCGeometry.createFromEsriCursor(cursor, SpatialReference.create(srid))
    }
  }

  /**
   * @return Number of rings if enclosed [[Geometry]] is a [[Polygon]], null otherwise
   */
  def numberOfRings = geom match {
    case (p: Polygon) +: Nil => p.getPathCount
    case _ => null
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
   *         NULL for empty geometry
   */
  def maxCoordinate(coord: Point => Double) =
    getCoordinateBoundary(coord, _ > _)

  /**
   * @param coord Coordinate extractor from point
   * @return minimum coordinate
   *         NULL for empty geometry
   */
  def minCoordinate(coord: Point => Double) =
    getCoordinateBoundary(coord, _ < _)

  /**
   * Returns a coordinate of the enclosed geometries satisfying some predicate
   * @param pred Predicate to satisfy (be the greatest, least, etc)
   * @param coord Function to extract the required coordinate from a point
   * @return
   */
  private def getCoordinateBoundary(coord: (Point => Double), pred: ((Double, Double) => Boolean)) = {
    def search: Seq[Geometry] => Double = {
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
        def expand: Int => Seq[Geometry] = {
          case i if i < x.getPointCount =>
            x.getPoint(i) +: expand(i + 1)
          case _ =>
            tail
        }
        search(expand(0))
    }
    if (geom.isEmpty) null else search(geom)
  }

  /**
   * @return JSON representation of the enclosed [[Geometry]]
   */
  def toJson =
    try {
      ogc.asJson
    } catch {
      case e: Exception =>
        GeometryValue.LOG.error("toJson failed", e)
        null
    }

  /**
   * @return GeoJSON representation of the enclosed [[Geometry]]
   */
  def toGeoJson =
    try {
      ogc.asGeoJson
    } catch {
      case e: Exception =>
        GeometryValue.LOG.error("toGeoJson failed", e)
        null
    }

  override def toString: String = toGeoJson
}

/**
 * Factory methods for [[GeometryValue]]
 */
object GeometryValue {
  private val LOG = Logger.getLogger(classOf[GeometryValue])

  /**
   * Default spatial reference
   */
  private val WGS84 = 4326

  /**
   * @param json
   * @return A [[GeometryValue]] built from a json string
   */
  def fromJson(json: String): GeometryValue = {
    new GeometryValue(OGCGeometry.fromJson(json))
  }

  /**
   * @param geoJson
   * @return A [[GeometryValue]] built from a geoJson string
   */
  def fromGeoJson(geoJson: String): GeometryValue = {
    new GeometryValue(OGCGeometry.fromGeoJson(geoJson))
  }

  /**
   * @param srid
   * @param xy
   * @return A [[GeometryValue]] enclosing a [[Point]]
   */
  def point(srid: Int, xy: (Double, Double)): GeometryValue =
    new GeometryValue(srid, GeometryBuilder.mkPoint(xy))

  /**
   * @param xy
   * @return A [[GeometryValue]] enclosing a [[Point]] with default SRID
   */
  def point(xy: (Double, Double)): GeometryValue =
    point(WGS84, xy)

  /**
   * @param srid
   * @param points
   * @return A [[GeometryValue]] enclosing a [[MultiPoint]]
   */
  def multiPoint(srid: Int, points: (Double, Double)*): GeometryValue = {
    new GeometryValue(srid, GeometryBuilder.mkMultiPoint(points))
  }

  /**
   * @param points
   * @return A [[GeometryValue]] enclosing a [[MultiPoint]] with default SRID
   */
  def multiPoint(points: (Double, Double)*): GeometryValue =
    multiPoint(WGS84, points: _*)

  /**
   * @param srid
   * @param points
   * @return A [[GeometryValue]] enclosing a [[Polyline]] with a single path
   */
  def line(srid: Int, points: (Double, Double)*): GeometryValue =
    new GeometryValue(srid, GeometryBuilder.mkLine(points))

  /**
   * @param points
   * @return A [[GeometryValue]] enclosing a [[Polyline]] with a single path and default SRID
   */
  def line(points: (Double, Double)*): GeometryValue =
    line(WGS84, points: _*)

  /**
   * @param srid
   * @param lines
   * @return A [[GeometryValue]] enclosing a [[Polyline]] with multiple paths
   */
  def multiLine(srid: Int, lines: Seq[(Double, Double)]*): GeometryValue =
    new GeometryValue(srid, GeometryBuilder.mkMultiLine(lines))

  /**
   * @param lines
   * @return A [[GeometryValue]] enclosing a [[Polyline]] with multiple paths and default SRID
   */
  def multiLine(lines: Seq[(Double, Double)]*): GeometryValue =
    multiLine(WGS84, lines: _*)

  /**
   * @param srid
   * @param points
   * @return A [[GeometryValue]] enclosing a [[Polygon]] with a single ring
   */
  def polygon(srid: Int, points: (Double, Double)*): GeometryValue =
    new GeometryValue(srid, GeometryBuilder.mkPolygon(points))

  /**
   * @param points
   * @return A [[GeometryValue]] enclosing a [[Polygon]] with a single ring and default SRID
   */
  def polygon(points: (Double, Double)*): GeometryValue =
    polygon(WGS84, points: _*)

  /**
   * @param srid
   * @param lines
   * @return A [[GeometryValue]] enclosing a [[Polygon]] with multiple rings
   */
  def multiPolygon(srid: Int, lines: Seq[(Double, Double)]*): GeometryValue =
    new GeometryValue(srid, GeometryBuilder.mkMultiPolygon(lines))

  /**
   * @param lines
   * @return A [[GeometryValue]] enclosing a [[Polygon]] with multiple rings and default SRID
   */
  def multiPolygon(lines: Seq[(Double, Double)]*): GeometryValue =
    multiPolygon(WGS84, lines: _*)

  /**
   * @param srid
   * @param geometries
   * @return A [[GeometryValue]] enclosing a geometry collection
   */
  def collection(srid: Int, geometries: Geometry*): GeometryValue = {
    val sr = SpatialReference.create(srid)
    val ogcGeoms = geometries.map(g => OGCGeometry.createFromEsriGeometry(g, sr))
    val geoColl = new OGCConcreteGeometryCollection(JavaConversions.seqAsJavaList(ogcGeoms), sr)
    new GeometryValue(geoColl)
  }
}
