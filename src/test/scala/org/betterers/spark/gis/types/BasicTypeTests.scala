package scala.org.betterers.spark.gis.types

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.test.TestSQLContext._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite

import org.betteres.spark.gis.types.Point

/**
 * Basic tests for GIS UDTs
 */
class BasicTypeTests extends FunSuite{
  val sqlContext = new SQLContext(sparkContext)

  test("Point: create RDD from runtime objects") {
    val points: Seq[Point] = Seq (new Point(1, 1), new Point(2, 1))
  }

  test("Point types from GeoJSON") {
    val points: Seq[Point] = Seq (new Point(1, 1), new Point(2, 1))

    val pointsRDD = sparkContext.parallelize( Seq(
      """{"x":1,"y":1,"spatialReference":{"wkid":4326}}""",
      """{"x":2,"y":2,"spatialReference":{"wkid":4326}}"""))
    val selectedRDD = sqlContext.jsonRDD(pointsRDD)
  }
}

