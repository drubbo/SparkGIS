package org.betterers.spark.gis

import java.io.CharArrayWriter

import org.apache.spark.Logging
import org.apache.spark.sql.types._
import org.codehaus.jackson.JsonFactory
import org.json.JSONException

/**
 * User defined type for [[Geometry]] instances
 *
 * @author drubbo <ubik@gamezoo.it>
 */
class GeometryType extends UserDefinedType[Geometry] with Logging {

  override def sqlType: DataType = StringType

  override def userClass: Class[Geometry] = classOf[Geometry]

  /**
   * Translates a [[Geometry]] to a geoJson [[String]]
   * @param obj
   * @return
   */
  override def serialize(obj: Any): String = {
    obj match {
      case g: Geometry =>
        g.toGeoJson
      case x =>
        throw new IllegalArgumentException("Invalid GeometryValue value to serialize: " + x)
    }
  }

  /**
   * Translates a [[Geometry]], a [[String]] containing a GeoJSON, or a [[Map]] obtained
   * during JSON deserialization to a [[Geometry]]
   * @param datum
   * @return
   */
  override def deserialize(datum: Any): Geometry = {
    datum match {
      case g: Geometry => g

      case s: String =>
        def tryOrElse[I,O](conversion: I => O, msg: String, alternative: I => O)(input: I): O = {
          try {
            conversion(input)
          } catch {
            case e: Throwable =>
              logWarning(msg, e)
              alternative(input)
          }
        }
        tryOrElse(
          Geometry.fromGeoJson,
          "Not a GeoJSON", tryOrElse(
            Geometry.fromJson,
            "Not a REST JSON", Geometry.fromString))(s)

      case r: Map[_, _] =>
        val writer = new CharArrayWriter()
        val gen = new JsonFactory().createJsonGenerator(writer)

        def writeJson: Any => Unit = {
          case m: Map[_, _] =>
            gen.writeStartObject()
            m.foreach { kv =>
              gen.writeFieldName(kv._1.toString)
              writeJson(kv._2)
            }
            gen.writeEndObject()
          case a: Seq[_] =>
            gen.writeStartArray()
            a.foreach(writeJson)
            gen.writeEndArray()
          case x =>
            gen.writeObject(x)
        }

        writeJson(r)
        gen.flush()
        val json = writer.toString
        gen.close()

        Geometry.fromGeoJson(json)

      case x =>
        throw new IllegalArgumentException("Can't deserialize to GeometryValue: " + x)
    }
  }
}

/**
 * Default [[GeometryType]] instance
 */
object GeometryType {

  val Instance = new GeometryType()

}
