package org.betterers.spark.gis

import com.esri.core.geometry.{MultiVertexGeometry, Segment, Point}

/**
 * [[Geometry]] support functions
 *
 * @author Ubik <emiliano.leporati@gmail.com>
 */
private[gis]
object Utils {

  /**
   * Returns an average coordinate of a set of points
   * @param coord coordinate extractor function
   * @param points
   * @return
   */
  def avgCoordinate(coord: (Point => Double), points: Seq[Point]): Double =
    points.map(coord).sum / points.size

  /**
   * @return Every point in an [[ESRIGeometry]]
   */
  def getPoints: ESRIGeometry => Seq[Point] = {
    case x: Point => Seq(x)
    case x: Segment => Seq(new Point(x.getStartX, x.getStartY), new Point(x.getEndX, x.getEndY))
    case x: MultiVertexGeometry => Seq.range(0, x.getPointCount - 1).map(x.getPoint)
  }

  /**
   * @return Length of a polyline described by a set of points
   */
  def getLength: Seq[Point] => Double = {
    case a +: b +: tail =>
      val dx = a.getX - b.getX
      val dy = a.getY - b.getY
      Math.sqrt(dx * dx + dy * dy) + getLength(b +: tail)
    case _ => 0
  }
}
