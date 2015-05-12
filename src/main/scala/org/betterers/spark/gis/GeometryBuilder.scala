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
   * Creates a [[Polyline]] with a single path from a sequence of coordinate pairs
   * @return
   */
  def mkLine(points: Seq[(Double, Double)]): Polyline = {
    addLine(new Polyline(), points)
  }

  /**
   * Creates a [[Polyline]] with multiple paths from a sequence of line points
   * @param lines
   * @return
   */
  def mkMultiLine(lines: Seq[Seq[(Double, Double)]]): Polyline = {
    addLines(new Polyline(), lines)
  }

  /**
   * Creates a [[Polygon]] with a single LinearRing.
   * Closing segment is added here, so avoid repeating the first point in last position.
   * @param points
   * @return
   */
  def mkPolygon(points: Seq[(Double, Double)]): Polygon = {
    val rt = addLine(new Polygon(), points)
    rt.closeAllPaths()
    rt
  }

  /**
   * Creates a [[Polygon]] with a set of LinearRing.
   * Closing segment is added here, so avoid repeating the first point in last position.
   * @param lines
   * @return
   */
  def mkMultiPolygon(lines: Seq[Seq[(Double, Double)]]): Polygon = {
    val rt = addLines(new Polygon(), lines)
    rt.closeAllPaths()
    rt
  }

  /**
   * Starts a new path in a [[MultiPath]] from the point provided
   * @param target
   * @param start
   * @tparam T
   * @return the target [[MultiPath]]
   */
  private def startSegment[T <: MultiPath](target: T, start: (Double, Double)): T = {
    target.startPath(mkPoint(start))
    target
  }

  /**
   * Adds a segment to a [[MultiPath]] in current from its last point to the one provided
   * @param target
   * @param end
   * @tparam T
   * @return the target [[MultiPath]]
   */
  private def addSegment[T <: MultiPath](target: T, end: (Double, Double)): T = {
    target.lineTo(mkPoint(end))
    target
  }

  /**
   * Adds a new path in a [[MultiPath]] defined by a sequence of coordinate pairs
   * @param target
   * @param points
   * @tparam T
   * @return
   */
  private def addLine[T <: MultiPath](target: T, points: Seq[(Double, Double)]): T = {
    points.tail.foldLeft(startSegment(target, points.head))(addSegment)
  }

  /**
   * Adds a new paths in a [[MultiPath]] defined by a sequence of sequence of coordinate pairs
   * @param target
   * @param lines
   * @tparam T
   * @return
   */
  private def addLines[T <: MultiPath](target: T, lines: Seq[Seq[(Double, Double)]]): T = {
    lines.foldLeft(target)(addLine)
  }

}
