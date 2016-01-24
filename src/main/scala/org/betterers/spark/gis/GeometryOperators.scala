package org.betterers.spark.gis

import scala.language.implicitConversions

/** Provides an implicit class to pimp the [[Geometry]] with
  * some symbolic operator as alias of some GIS function
  *
  * Import it in your scope to enable the implicit conversion:
  * {{{
  *   import GeometryOperators._
  *   val l1 = Geometry.line((10,10),(20,20),(10,30))
  *   val l2 = Geometry.line((100,100),(200,200))
  *   if (l1 ^ l2) println("that's weird")
  * }}}
  *
  * @author Ubik <emiliano.leporati@gmail.com>
  */
object GeometryOperators {

  /** Augmented [[Geometry]] with operators aliasing some functionality provided by [[udf.Functions]] */
  implicit class GeometryOperators(geom: Geometry) {

    import Geometry.ImplicitConversions._

    /** Same as [[udf.Functions.ST_Distance]] */
    def <->(other: Geometry): Double = geom.distance(other)

    /** Same as [[udf.Functions.ST_Difference]] */
    def -(other: Geometry): Geometry = geom.difference(other)

    /** Same as [[udf.Functions.ST_Union]] */
    def +(other: Geometry): Geometry = geom.union(other)

    /** Same as [[udf.Functions.ST_Intersection]] */
    def ^(other: Geometry): Geometry = geom.intersection(other)

    /** Same as [[udf.Functions.ST_SymDifference]] */
    def |-|(other: Geometry): Geometry = geom.symmetricDifference(other)

    /** Same as [[udf.Functions.ST_Contains]] */
    def >(other: Geometry): Boolean = geom.contains(other)

    /** Same as [[udf.Functions.ST_Within]] */
    def <(other: Geometry): Boolean = geom.within(other)

    /** Same as [[udf.Functions.ST_Crosses]] */
    def ><(other: Geometry): Boolean = geom.crosses(other)

    /** Same as [[udf.Functions.ST_Disjoint]] */
    def <>(other: Geometry): Boolean = geom.disjoint(other)

    /** Same as [[udf.Functions.ST_Equals]] */
    def ==(other: Geometry): Boolean = geom.equals(other)

    /** Same as [[udf.Functions.ST_Equals]] */
    def !=(other: Geometry): Boolean = !geom.equals(other)

    /** Same as [[udf.Functions.ST_Intersects]] */
    def |^|(other: Geometry): Boolean = geom.intersects(other)

    /** Same as [[udf.Functions.ST_Overlaps]] */
    def |@|(other: Geometry): Boolean = geom.overlaps(other)

    /** Same as [[udf.Functions.ST_Touches]] */
    def ||(other: Geometry): Boolean = geom.touches(other)

    /** Same as [[udf.Functions.ST_AsEWKT]] */
    override def toString = geom.toString

    /** Tells if this 2D geometry is above another */
    def ?/(other: Geometry): Boolean = testPosition(geom.yMin, _ > _, other.yMax)

    /** Tells if this 2D geometry is below another */
    def ?\(other: Geometry): Boolean = testPosition(geom.yMax, _ < _, other.yMin)

    /** Tells if this 2D geometry is on the right side of another */
    def ?>(other: Geometry): Boolean = testPosition(geom.xMin, _ > _, other.xMax)

    /** Tells if this 2D geometry is on the left side of another */
    def ?<(other: Geometry): Boolean = testPosition(geom.xMax, _ < _, other.xMin)

    /** Utility to test relative position of two 2D geometries */
    private def testPosition(myCoord: Option[Double],
                             predicate: (Double, Double) => Boolean,
                             hisCoord: Option[Double]): Boolean =
      (for {
        mine <- myCoord
        his <- hisCoord
      } yield predicate(mine, his)).getOrElse(false)
  }

}
