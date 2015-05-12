package org.betteres.spark.gis.types

import com.esri.core.geometry.ogc.OGCGeometry
import com.esri.core.geometry.{GeometryEngine, Operator, OperatorFactoryLocal, OperatorImportFromGeoJson}
import org.apache.spark.sql.types._
import org.codehaus.jackson.JsonFactory

/**
 * Created by emilianl on 5/11/15.
 */
@SQLUserDefinedType(udt = classOf[Geometry])
class Geometry extends UserDefinedType[Geometry] {

  private var value: OGCGeometry = null

  override def sqlType: DataType = StringType

  override def userClass: Class[Geometry] = classOf[Geometry]

  /**
   * Translates a [[Geometry]] to a geoJson [[String]]
   * @param obj
   * @return
   */
  override def serialize(obj: Any): Any = {
    obj match {
      case g: Geometry =>
        g.value.asJson()
      case _ =>
        throw new IllegalArgumentException("Invalid type for Geometry")
    }
  }

  /**
   * Translates a [[Geometry]] or [[String]] datum to a [[Geometry]]
   * @param datum
   * @return
   */
  override def deserialize(datum: Any): Geometry = {
    datum match {
      case g: Geometry => g
      case s: String => Geometry.fromJson(s)
      case _ =>
        throw new IllegalArgumentException("Invalid serialization for Geometry")
    }
  }
}

/**
 * Factory methods for [[Geometry]]
 */
object Geometry {

  val Type: Geometry = new Geometry

  /**
   * Build a [[Geometry]] from an [[OGCGeometry]]
   * @param g
   * @return
   */
  def apply(g: OGCGeometry): Geometry = {
    val result = new Geometry {
      value = g
    }
    return result
  }

  /**
   * Build a [[Geometry]] from a json string
   * @param json
   * @return
   */
  def fromJson(json: String): Geometry = {
    val factory = new JsonFactory()
    val jsonParserPt = factory.createJsonParser(json)
    jsonParserPt.nextToken()
    val mapGeom = GeometryEngine.jsonToGeometry(jsonParserPt);
    Geometry(OGCGeometry.createFromEsriGeometry(mapGeom.getGeometry(),
      mapGeom.getSpatialReference()))
  }


  /**
   * Build a [[Geometry]] from a geoJson string
   * @param geoJson
   * @return
   */
  def fromGeoJson(geoJson: String): Geometry = {
    val op = OperatorFactoryLocal.getInstance.getOperator(Operator.Type.ImportFromJson).asInstanceOf[OperatorImportFromGeoJson];
    val mapOGCStructure = op.executeOGC(0, geoJson, null);

    Geometry(OGCGeometry.createFromOGCStructure(
      mapOGCStructure.m_ogcStructure,
      mapOGCStructure.m_spatialReference))
  }
}
