package locationspark.measurements

import cs.purdue.edu.spatialrdd.SpatialRDD
import cs.purdue.edu.spatialindex.rtree._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import com.vividsolutions.jts.io.WKTReader
//import cs.purdue.edu.spatialindex.qtree.{Box, Point}
import cs.purdue.edu.spatialindex.rtree.{Box, Entry, Point}
import cs.purdue.edu.spatialrdd.SpatialRDD
import cs.purdue.edu.spatialrdd.impl.Util
import org.apache.spark.{SparkConf, SparkContext}
import scala.util.Try

object SpatialJoins {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("LocationSpark Spatial Joins")
    val sc = new SparkContext(conf)
    var count = 0L
    sc.setLogLevel("OFF")
    Util.localIndex = "QTREE"
    var t0 = 0L
    var t1 = 0L

    def aggfunction1[K, V](itr: Iterator[(K, V)]): Int = {
      itr.size
    }

    def aggfunction2(v1: Int, v2: Int): Int = {
      v1 + v2
    }

    t0 = System.nanoTime()
    val leftpoints = sc.textFile("/data/points_200M_wkt.csv", 1024).map(x => (Try(new WKTReader().read(x)))).filter(_.isSuccess).map {
      case x =>
        val corrds = x.get.getCoordinates
        val p1 = corrds(0)
        (Point(p1.x.toFloat, p1.y.toFloat), "1")
    }
    val leftLocationRDD = SpatialRDD(leftpoints).cache()
    count = leftLocationRDD.count()
    t1 = System.nanoTime()
    val leftTime = (t1 - t0) / 1E9
    println("Left Indexing Time: " + ((t1 - t0) / 1E9) + " sec")

    t0 = System.nanoTime()
    val rightData = sc.textFile("/data/rectangles_114M.csv")
    val rightBoxes = rightData.map(x => (Try(new WKTReader().read(x)))).filter(_.isSuccess).map {
      case x =>
        val corrds = x.get.getCoordinates
        val p1 = corrds(0)
        val p2 = corrds(2)
        Box(p1.x.toFloat, p1.y.toFloat, p2.x.toFloat, p2.y.toFloat)
    }.cache()
    val count1 = rightBoxes.count()
    t1 = System.nanoTime()
    val rightTime = (t1 - t0) / 1E9
    println("Right Indexing Time: " + rightTime + " sec")

    t0 = System.nanoTime()
    val joinresultRdd0 = leftLocationRDD.rjoin(rightBoxes)(aggfunction1, aggfunction2)
    val tuples0 = joinresultRdd0.map { case (b, v) => (1, v) }.reduceByKey { case (a, b) => {
      a + b
    }
    }.map { case (a, b) => b }.collect()
    val count0 = tuples0(0)
    t1 = System.nanoTime()
    val time0 = (t1 - t0) / 1E9

    val total_time = leftTime + rightTime + time0
    println("Total Join Time: " + total_time + " sec")
  }
}