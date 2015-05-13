package org.betterers.spark.gis

import com.esri.core.geometry._

/**
 * Factory methods to build [[Geometry]] values from sets of coordinate pairs
 *
 * @author drubbo <ubik@gamezoo.it>
 */
object GeometryBuilder {

  /**
   * Creates a [[Point]] from a pair of coordinates
   * @return
   */
  def mkPoint: ((Double, Double)) => Point = {
    case (x, y) => new Point(x, y)
  }

  /**
   * Creates a [[MultiPoint]] from a sequence of coordinate pairs
   * @param points
   * @return
   */
  def mkMultiPoint(points: Seq[(Double, Double)]): MultiPoint = {
    val rt = new MultiPoint()
    points.map(mkPoint).foreach(rt.add)
    rt
  }

  /**
   * Creates a [[Polyline]] with a single path from a sequence of coordinate pairs
   * @return
   */
  def mkLine(points: Seq[(Double, Double)]): Polyline = {
    val rt = new Polyline()
    addLine(false)(rt, points)
    rt
  }

  /**
   * Creates a [[Polyline]] with multiple paths from a sequence of line points
   * @param lines
   * @return
   */
  def mkMultiLine(lines: Seq[Seq[(Double, Double)]]): Polyline = {
    val rt = new Polyline()
    addLines(false)(rt, lines)
    rt
  }

  /**
   * Creates a [[Polygon]] with a single LinearRing.
   * Closing segment is added here, so avoid repeating the first point in last position.
   * @param points
   * @return
   */
  def mkPolygon(points: Seq[(Double, Double)]): Polygon = {
    val rt = new Polygon()
    addLine(true)(rt, points)
    rt
  }

  /**
   * Creates a [[Polygon]] with a set of LinearRing.
   * Closing segment is added here, so avoid repeating the first point in last position.
   * @param lines
   * @return
   */
  def mkMultiPolygon(lines: Seq[Seq[(Double, Double)]]): Polygon = {
    val rt = new Polygon()
    addLines(true)(rt, lines)
    rt
  }

  /**
   * Adds a new path in a [[MultiPath]] defined by a sequence of coordinate pairs
   * @param target
   * @param points
   * @tparam T
   * @return
   */
  private def addLine[T <: MultiPath](closed: Boolean)(target: T, points: Seq[(Double, Double)]): Unit = {
    target.startPath(points.head._1, points.head._2)
    points.tail.foreach(p => target.lineTo(p._1, p._2))
    if (closed) target.closePathWithLine()
  }

  /**
   * Adds a new paths in a [[MultiPath]] defined by a sequence of sequence of coordinate pairs
   * @param target
   * @param lines
   * @tparam T
   * @return
   */
  private def addLines[T <: MultiPath](closed: Boolean)(target: T, lines: Seq[Seq[(Double, Double)]]): Unit =
    lines.foreach(l => addLine(closed)(target, l))

}
