package org.betterers.spark.gis

import com.esri.core.geometry._

/** [[Geometry]] support functions
  *
  * @author Ubik <emiliano.leporati@gmail.com>
  */
private[gis]
object Utils {

  /** Returns an average coordinate of a set of points
    *
    * @param coord coordinate extractor function
    */
  def avgCoordinate(coord: (Point => Double), points: Seq[Point]): Double =
    points.map(coord).sum / points.size

  /** Returns an average coordinate of a set of weighted points
    *
    * @param coord coordinate extractor function
    * @param pointsAndWeigths points and their weights
    */
  def avgWeightCoordinate(coord: (Point => Double), pointsAndWeigths: (Seq[Point], Seq[Double])): Double =
    pointsAndWeigths._1.map(coord).sum / pointsAndWeigths._2.sum

  /** Returns every point in an [[ESRIGeometry]] */
  def getPoints: ESRIGeometry => Seq[Point] = {
    case x: Point => Seq(x)
    case x: Segment => Seq(new Point(x.getStartX, x.getStartY), new Point(x.getEndX, x.getEndY))
    case x: MultiVertexGeometry => Seq.range(0, x.getPointCount).map(x.getPoint)
  }

  /** Returns every point of a specific path in a [[MultiPath]] */
  def getPoints(geom: MultiPath, pathIndex: Int): Seq[Point] =
    Range(geom.getPathStart(pathIndex), geom.getPathEnd(pathIndex)).map(geom.getPoint)

  /** Returns the length of a polyline described by a set of points */
  def getLength: Seq[Point] => Double = {
    case a +: b +: tail =>
      val dx = a.getX - b.getX
      val dy = a.getY - b.getY
      Math.sqrt(dx * dx + dy * dy) + getLength(b +: tail)
    case _ => 0
  }

  /** Returns coordinates of a [[Point]] as a pair */
  def getCoordinates(p: Point): Geometry.Coordinates =
    (p.getX, p.getY)
}
