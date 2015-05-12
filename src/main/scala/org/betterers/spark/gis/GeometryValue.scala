package org.betterers.spark.gis

import com.esri.core.geometry._
import com.esri.core.geometry.ogc.OGCGeometry
import org.apache.spark.sql.types.SQLUserDefinedType
import org.codehaus.jackson.JsonFactory

/**
 * Class wrapping an ESRI [[Geometry]] representing values of [[GeometryType]]
 *
 * @author drubbo <ubik@gamezoo.it>
 */
@SQLUserDefinedType(udt = classOf[GeometryType])
class GeometryValue(val srid: Int, val geom: Geometry) extends Serializable {
  /**
   * Builds a [[GeometryValue]] from an ESRI [[Geometry]] with default SRID
   * @param geom
   */
  def this(geom: Geometry) = {
    this(GeometryValue.WGS84, geom)
  }

  /**
   * Builds a [[GeometryValue]] of type Point, LineString, or MultiLineString, with an SRID
   * @param srid
   * @param points One or a sequence of coordinate pairs
   */
  def this(srid: Int, points: (Double, Double)*) = {
    this(srid, {
      points match {
        case (l: Seq[_]) +: Nil =>
          GeometryBuilder.mkLine(l.asInstanceOf[Seq[(Double, Double)]])
        case (_: Seq[_]) +: _ =>
          GeometryBuilder.mkMultiLine(points.asInstanceOf[Seq[Seq[(Double, Double)]]])
        case p +: Nil =>
          GeometryBuilder.mkPoint(p)
        case _ +: _ =>
          GeometryBuilder.mkLine(points)
        case _ =>
          throw new IllegalArgumentException("Invalid points to build a Geometry: " + points)
      }
    })
  }

  /**
   * Builds a [[GeometryValue]] of type Point, LineString, or MultiLineString, with default SRID
   * @param points One or a sequence of coordinate pairs
   */
  def this(points: (Double, Double)*) = {
    this(GeometryValue.WGS84, points: _*)
  }

  /**
   * @return Type of enclosed [[Geometry]]
   */
  def geometryType: Geometry.Type = geom.getType

  /**
   * @return [[OGCGeometry]] from the enclosed [[Geometry]]
   */
  def ogc = OGCGeometry.createFromEsriGeometry(geom, SpatialReference.create(srid))

  /**
   * @return JSON representation of the enclosed [[Geometry]]
   */
  def toJson =
    if (geom == null) null
    else GeometryEngine.geometryToJson(srid, geom)

  /**
   * @return GeoJSON representation of the enclosed [[Geometry]]
   */
  def toGeoJson =
    if (geom == null) null
    else {
      val op = OperatorFactoryLocal.getInstance().
        getOperator(Operator.Type.ExportToGeoJson).
        asInstanceOf[OperatorExportToGeoJson]
      op.execute(SpatialReference.create(srid), geom)
    }

  override def toString: String = toGeoJson
}

/**
 * Factory methods for [[GeometryValue]]
 */
object GeometryValue {

  /**
   * Default spatial reference
   */
  private val WGS84 = 4326

  /**
   * @param json
   * @return A [[GeometryValue]] built from a json string
   */
  def fromJson(json: String): GeometryValue = {
    val jsonParser = new JsonFactory().createJsonParser(json)
    jsonParser.nextToken()
    val geom = GeometryEngine.jsonToGeometry(jsonParser)
    new GeometryValue(geom.getSpatialReference.getID, geom.getGeometry)
  }

  /**
   * @param geoJson
   * @return A [[GeometryValue]] built from a geoJson string
   */
  def fromGeoJson(geoJson: String): GeometryValue = {
    val op = OperatorFactoryLocal.getInstance.
      getOperator(Operator.Type.ImportFromGeoJson).
      asInstanceOf[OperatorImportFromGeoJson]
    val geom = op.execute(0, Geometry.Type.Unknown, geoJson, null)

    new GeometryValue(geom.getSpatialReference.getID, geom.getGeometry)
  }

  /**
   * @param xy
   * @return A [[GeometryValue]] enclosing a [[Point]]
   */
  def point(xy: (Double, Double)) =
    new GeometryValue(GeometryBuilder.mkPoint(xy))

  /**
   * @param points
   * @return A [[GeometryValue]] enclosing a [[Polyline]] with a single path
   */
  def line(points: (Double, Double)*) =
    new GeometryValue(GeometryBuilder.mkLine(points))

  /**
   * @param lines
   * @return A [[GeometryValue]] enclosing a [[Polyline]] with multiple paths
   */
  def multiLine(lines: Seq[(Double, Double)]*) =
    new GeometryValue(GeometryBuilder.mkMultiLine(lines))

  /**
   * @param points
   * @return A [[GeometryValue]] enclosing a [[Polygon]] with a single ring
   */
  def polygon(points: (Double, Double)*) =
    new GeometryValue(GeometryBuilder.mkPolygon(points))

  /**
   * @param lines
   * @return A [[GeometryValue]] enclosing a [[Polygon]] with multiple rings
   */
  def multiPolygon(lines: Seq[(Double, Double)]*) =
    new GeometryValue(GeometryBuilder.mkMultiPolygon(lines))
}
