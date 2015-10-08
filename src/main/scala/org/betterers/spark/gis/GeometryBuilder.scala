package org.betterers.spark.gis

import com.esri.core.geometry._

/**
 * Factory methods to build [[ESRIGeometry]] values from sets of coordinate pairs
 *
 * @author Ubik <emiliano.leporati@gmail.com>
 */
object GeometryBuilder {

  import Geometry.Coordinates

  /** Creates a [[Point]] from a pair of coordinates */
  def mkPoint: (Coordinates) => Point = {
    case (x, y) => new Point(x, y)
  }

  /** Creates a [[MultiPoint]] from a sequence of coordinate pairs */
  def mkMultiPoint(points: Seq[Coordinates]): MultiPoint = {
    val rt = new MultiPoint()
    points.map(mkPoint).foreach(rt.add)
    rt
  }

  /** Creates a [[Polyline]] with a single path from a sequence of coordinate pairs */
  def mkLine(points: Seq[Coordinates]): Polyline = {
    val rt = new Polyline()
    addLine(closed = false)(rt, points)
    rt
  }

  /** Creates a [[Polyline]] with multiple paths from a sequence of line points */
  def mkMultiLine(lines: Seq[Seq[Coordinates]]): Polyline = {
    val rt = new Polyline()
    addLines(closed = false)(rt, lines)
    rt
  }

  /** Creates a [[Polygon]] with a single LinearRing.
    *
    * The closing segment is added here, so there's no need to repeat the first point
    * in last position when calling this.
    */
  def mkPolygon(points: Seq[Coordinates]): Polygon = {
    val rt = new Polygon()
    addLine(closed = true)(rt, points)
    rt
  }

  /** Creates a [[Polygon]] with a set of LinearRing.
    *
    * The closing segment is added here, so there's no need to repeat the first point
    * in last position when calling this.
    */
  def mkMultiPolygon(lines: Seq[Seq[Coordinates]]): Polygon = {
    val rt = new Polygon()
    addLines(closed = true)(rt, lines)
    rt
  }

  /** Helper: adds a new path in a [[MultiPath]] defined by a sequence of coordinate pairs */
  private def addLine[T <: MultiPath](closed: Boolean)(target: T, points: Seq[Coordinates]): Unit = {
    target.startPath(points.head._1, points.head._2)
    points.tail.foreach(p => target.lineTo(p._1, p._2))
    if (closed) target.closePathWithLine()
  }

  /** Helper: adds a new paths in a [[MultiPath]] defined by a sequence of sequence of coordinate pairs */
  private def addLines[T <: MultiPath](closed: Boolean)(target: T, lines: Seq[Seq[Coordinates]]): Unit =
    lines.foreach(l => addLine(closed)(target, l))

}
