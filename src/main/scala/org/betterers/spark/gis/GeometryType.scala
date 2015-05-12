package org.betterers.spark.gis

import java.io.CharArrayWriter

import org.apache.spark.sql.types._
import org.codehaus.jackson.JsonFactory

/**
 * User defined type for [[GeometryValue]] instances
 *
 * @author drubbo <ubik@gamezoo.it>
 */
class GeometryType extends UserDefinedType[GeometryValue] {

  override def sqlType: DataType = StringType

  override def userClass: Class[GeometryValue] = classOf[GeometryValue]

  /**
   * Translates a [[GeometryValue]] to a geoJson [[String]]
   * @param obj
   * @return
   */
  override def serialize(obj: Any): String = {
    obj match {
      case g: GeometryValue =>
        g.toGeoJson
      case x =>
        throw new IllegalArgumentException("Invalid GeometryValue value to serialize: " + x)
    }
  }

  /**
   * Translates a [[GeometryValue]], a [[String]] containing a GeoJSON, or a [[Map]] obtained
   * during JSON deserialization to a [[GeometryValue]]
   * @param datum
   * @return
   */
  override def deserialize(datum: Any): GeometryValue = {
    datum match {
      case g: GeometryValue => g
      case s: String => GeometryValue.fromGeoJson(s)
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

        GeometryValue.fromGeoJson(json)
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
