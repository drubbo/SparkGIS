package org.betterers.spark.gis

import java.nio.ByteBuffer

import com.vividsolutions.jts.geom.{Coordinate => Coord, Geometry => Geom, _}
import com.vividsolutions.jts.io.WKBReader
import org.geotools.geojson.geom.GeometryJSON
import org.geotools.geometry.jts.WKTReader2

import scala.language.implicitConversions

/**
 * Implicit methods to convert between GeoTools and SparkGIS types
 *
 * @author Ubik <emiliano.leporati@gmail.com>
 */
private[gis]
object GeometryConversions {

  import GisGeometry.ImplicitConversions.fromGeom

  /** Factory for GeoTools geometries */
  private lazy val geoTools = new GeometryFactory()

  /** Returns a sequence of [[Coordinate]] where the last one is ensured to be equal to the first */
  private def closeRing(coordinates: Seq[Coordinate]): Seq[Coordinate] =
    if (coordinates.head == coordinates.tail) coordinates
    else coordinates :+ coordinates.head

  /** Builds a geometry from a `GeoJSON` string */
  def fromGeoJSON(geoJson: String): Geometry =
    new GeometryJSON().read(geoJson)

  /** Builds a geometry from a `WKB` */
  def fromBinary(wkb: ByteBuffer): Geometry =
    new WKBReader(geoTools).read(wkb.array())

  /** Builds a geometry from a `WKT` string */
  def fromString(wkt: String): Geometry =
    new WKTReader2(geoTools).read(wkt)

  /** Builds a [[Geometry]] from a GeoTools one */
  implicit def toGeometry(geom: Geom): Geometry =
    new Geometry((geom, geom.getSRID))

  /** Builds a [[Geometry]] option from a GeoTools one */
  implicit def toGeometry(geom: Option[Geom]): Option[Geometry] =
    geom.map(g => new Geometry(g, g.getSRID))

  /** Builds a [[Geometry]] from a GeoTools one paired with an SRID */
  implicit def toGeometry(geom: (Geom, Int)): Geometry =
    new Geometry(geom._1, geom._2)

  /** Converts a GeoTools [[Point]] to a [[Coordinate]] */
  implicit def toCoordinate(p: Point): Coordinate =
    (p.getX, p.getY)

  /** Converts a [[Coordinate]] to a GeoTools one */
  implicit def toCoord: Coordinate => Coord = {
    case (x, y) => new Coord(x, y)
  }

  /** Converts a sequence of [[Coordinate]] to an array of GeoTools ones */
  implicit def toCoord(s: Seq[Coordinate]): Array[Coord] =
    s.map({ a => a: Coord }).toArray

  /** Converts a [[Coordinate]] to a [[Point]] */
  implicit def toPoint: Coordinate => Point =
    geoTools.createPoint(_)

  /** Converts a sequence of [[Coordinate]] to a [[MultiPoint]] */
  implicit def toMultiPoint: Seq[Coordinate] => MultiPoint =
    geoTools.createMultiPoint(_)

  /** Converts a sequence of [[Coordinate]] to a [[LineString]] with a single path */
  implicit def toLine: Seq[Coordinate] => LineString =
    geoTools.createLineString(_)

  /** Converts a sequence of sequences of [[Coordinate]] to an array of [[LineString]]s */
  implicit def toLines(s: Seq[Seq[Coordinate]]): Array[LineString] =
    s.map(toLine).toArray

  /** Converts a sequence of sequences of [[Coordinate]] to a single [[MultiLineString]]s */
  implicit def toMultiLine: Seq[Seq[Coordinate]] => MultiLineString =
    geoTools.createMultiLineString(_)

  /** Converts a sequence of [[Coordinate]] to a [[Polygon]] with a single ring */
  implicit def toPolygon(s: Seq[Coordinate]): Polygon =
    geoTools.createPolygon(closeRing(s))

  /** Converts a sequence of sequences of [[Coordinate]] to an array of [[Polygon]]s */
  implicit def toPolygons(s: Seq[Seq[Coordinate]]): Array[Polygon] =
    s.map(toPolygon).toArray

  /** Converts a sequence of [[Coordinate]] to a [[MultiPolygon]] */
  implicit def toMultiPolygon: Seq[Seq[Coordinate]] => MultiPolygon =
    geoTools.createMultiPolygon(_)

  /** Converts a sequence of [[Geometry]] to a [[GeometryCollection]] */
  implicit def toGeometryCollection: Seq[Geometry] => GeometryCollection =
    c => geoTools.createGeometryCollection(c.map(_.impl.geom).toArray)
}
