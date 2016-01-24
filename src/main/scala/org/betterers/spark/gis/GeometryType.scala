package org.betterers.spark.gis

import java.io.CharArrayWriter
import java.nio.ByteBuffer

import org.apache.spark.Logging
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.codehaus.jackson.JsonFactory

import scala.language.postfixOps
import scala.util.Try

/** User defined type for [[Geometry]] instances
  *
  * @author Ubik <emiliano.leporati@gmail.com>
  */
class GeometryType extends UserDefinedType[Geometry] with Logging {

  /** [[Geometry]] values are serialized as strings */
  override def sqlType: DataType = StringType

  override def userClass: Class[Geometry] = classOf[Geometry]

  /** Translates a [[Geometry]] to a GeoJSON [[String]] */
  override def serialize(obj: Any): String = {
    obj match {
      case g: Geometry =>
        g.impl.toGeoJson
      case _ =>
        throw new IllegalArgumentException(s"Invalid Geometry value to serialize: $obj")
    }
  }

  /** Translates a [[Geometry]], a [[String]] containing a GeoJSON, or a [[Map]] obtained
    * during JSON deserialization to a [[Geometry]]
    */
  override def deserialize(datum: Any): Geometry = {
    datum match {
      case g: Geometry => g

      case b: Array[Byte] => Geometry.fromBinary(b)
      case b: ByteBuffer => Geometry.fromBinary(b)

      case s: UTF8String => deserialize(s.toString)
      case s: String =>
        Try {
          Geometry.fromGeoJson(s)
        } orElse {
          logWarning("Not a GeoJSON")
          Try(Geometry.fromString(s))
        } get

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
        throw new IllegalArgumentException(s"Can't deserialize to Geometry: ${x.getClass.getSimpleName}: $x")
    }
  }
}

/** Default [[GeometryType]] instance */
object GeometryType {

  /** [[GeometryType]] instance to be used in schemas */
  val Instance = new GeometryType()

}
