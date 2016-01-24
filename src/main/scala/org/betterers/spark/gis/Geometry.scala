package org.betterers.spark.gis

import java.nio.ByteBuffer

import com.vividsolutions.jts.geom.{Geometry => Geom, _}
import org.apache.spark.sql.types.SQLUserDefinedType

import scala.language.implicitConversions

/** Class wrapping a [[Geom]] representing values of [[GeometryType]]
  *
  * @constructor Builds a new geometry given an SRID and a [[Geometry]]
  * @author Ubik <emiliano.leporati@gmail.com>
  */
@SQLUserDefinedType(udt = classOf[GeometryType])
class Geometry(private[gis] val impl: GisGeometry)
  extends Serializable {

  /** Returns the WKT representation of this geometry */
  override def toString: String = impl.toString

  /** Forwards equality to equality of implementations */
  override def equals(other: Any) = other match {
    case g: Geometry => impl.equals(g.impl)
    case _ => false
  }
}

/** Factory methods for [[Geometry]] */
object Geometry {

  /** Default spatial reference */
  implicit val WGS84 = 4326

  import GeometryConversions._

  /** Builds a geometry from a `GeoJSON` string */
  def fromGeoJson(geoJson: String): Geometry =
    GeometryConversions.fromGeoJSON(geoJson)

  /** Builds a geometry from a `WKB` */
  def fromBinary(wkb: ByteBuffer): Geometry =
    GeometryConversions.fromBinary(wkb)

  /** Builds a geometry from a `WKB` */
  def fromBinary(wkb: Array[Byte]): Geometry =
    fromBinary(ByteBuffer.wrap(wkb))

  /** Builds a geometry from a `WKT` string */
  def fromString(wkt: String): Geometry =
    GeometryConversions.fromString(wkt)

  /** Builds a geometry wrapping a [[Point]] */
  def point(xy: Coordinate)(implicit srid: Int): Geometry =
    (xy: Point, srid)

  /** Builds a geometry wrapping a [[Point]] */
  def point(x: Double, y: Double)(implicit srid: Int): Geometry =
    ((x, y): Point, srid)

  /** Builds a geometry wrapping a [[MultiPoint]] with default SRID */
  def multiPoint(points: Coordinate*)(implicit srid: Int): Geometry =
    (points: MultiPoint, srid)

  /** Builds a geometry wrapping a [[LineString]] with a single path */
  def line(points: Coordinate*)(implicit srid: Int): Geometry =
    (points: LineString, srid)

  /** Builds a geometry wrapping a [[MultiLineString]] with multiple paths and default SRID */
  def multiLine(lines: Seq[Coordinate]*)(implicit srid: Int): Geometry =
    (lines: MultiLineString, srid)

  /** Builds a geometry wrapping a [[Polygon]] with a single ring */
  def polygon(points: Coordinate*)(implicit srid: Int): Geometry =
    (points: Polygon, srid)

  /** Builds a geometry wrapping a [[Polygon]] with multiple rings */
  def multiPolygon(lines: Seq[Coordinate]*)(implicit srid: Int): Geometry =
    (lines: MultiPolygon, srid)

  /** Builds a geometry wrapping a geometry collection
    * combining every [[Geometry]] enclosed in argument list.
    */
  def collection(geometries: Geometry*)(implicit srid: Int): Geometry =
    (geometries: GeometryCollection, srid)

  /** Implicit conversions giving access to the [[GisGeometry]] interface wrapped by [[Geometry]]
    *
    * These are useful if you plan to use the [[Geometry]] through an OO interface rather than
    * through the standard GIS ST_ functions; since [[GisGeometry]] provides such interface, conversions
    * back and forth are required.
    */
  object ImplicitConversions {
    implicit def toImpl(g: Geometry): GisGeometry = GisGeometry.ImplicitConversions.fromGeometry(g)

    implicit def fromImpl(g: GisGeometry): Geometry = new Geometry(g)

    implicit def fromImplOption(g: Option[GisGeometry]): Option[Geometry] = g.map(new Geometry(_))
  }

}
