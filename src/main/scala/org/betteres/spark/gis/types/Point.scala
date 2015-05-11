package org.betteres.spark.gis.types

import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

/**
 * Point SQL UDT
 */
@SQLUserDefinedType(udt=classOf[Point])
class Point(xc:Double, yc:Double) extends UserDefinedType[Point]{

  var x = xc
  var y = yc

  override def sqlType: StructType = {
    StructType(
        Seq(
          StructField("x",DoubleType, nullable=false),
          StructField("y",DoubleType, nullable=false)
        )
    )

  }

  override def userClass: Class[Point] = classOf[Point]

  override def serialize(obj: Any): Row = {
    val row = new GenericMutableRow(2)
    row.setDouble(0, x)
    row.setDouble(1, y)
    row
  }

  override def deserialize(datum:Any): Point = {
    datum match {
      case p : Point =>
        p
      case row: Row =>
        require(row.length == 2, s"Point.deserialize given row with length ${row.length} but requires length == 2")
        val x = row.getDouble(0)
        val y = row.getDouble(1)
        new Point(x, y)
    }
  }
}
