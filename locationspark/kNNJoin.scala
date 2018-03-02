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
import cs.purdue.edu.spatialrdd.impl.{Util, knnJoinRDD}
import org.apache.spark.{SparkConf, SparkContext}
import scala.util.Try

object kNNJoin {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("LocationSpark kNN Join")
    val sc = new SparkContext(conf)
    var count = 0L
    sc.setLogLevel("OFF")
    Util.localIndex = "QTREE"
    var t0 = 0L
    var t1 = 0L
    val knn = 5

    def aggfunction1[K, V](itr: Iterator[(K, V)]): Int = {
      itr.size
    }

    def aggfunction2(v1: Int, v2: Int): Int = {
      v1 + v2
    }

    t0 = System.nanoTime()
    /** **********************************************************************************/
    val leftpoints = sc.textFile("/data/points_10M_wkt.csv").map(x => (Try(new WKTReader().read(x))))
      .filter(_.isSuccess).map {
      case x =>
        val corrds = x.get.getCoordinates
        val p1 = corrds(0)
        (Point(p1.x.toFloat, p1.y.toFloat), "1")
    }
    val leftLocationRDD = SpatialRDD(leftpoints).cache()
    /** **********************************************************************************/
    count = leftLocationRDD.count()
    t1 = System.nanoTime()
    val leftTime = (t1 - t0) / 1E9
    println("Left Indexing Time: " + ((t1 - t0) / 1E9) + " sec")

    t0 = System.nanoTime()
    /** **********************************************************************************/
    val rightpoints = sc.textFile("/data/points_10M_wkt.csv").map(x => (Try(new WKTReader().read(x))))
      .filter(_.isSuccess).map {
      case x =>
        val corrds = x.get.getCoordinates
        val p1 = corrds(0)
        (Point(p1.x.toFloat, p1.y.toFloat))
    }.cache()
    /** **********************************************************************************/
    val count1 = rightpoints.count()
    t1 = System.nanoTime()
    val rightTime = (t1 - t0) / 1E9
    println("Right Indexing Time: " + rightTime + " sec")


    t0 = System.nanoTime()

    val knnjoin = new knnJoinRDD[Point, String](leftLocationRDD, rightpoints, knn, (id) => true, (id) => true)

    val knnjoinresult = knnjoin.rangebasedKnnjoin()

    val tuples = knnjoinresult.map { case (b, v) => (1, v.size) }.reduceByKey { case (a, b) => {
      a + b
    }
    }.map { case (a, b) => b }.collect()

    println("k: " + knn)
    println("kNN Join Results Size: " + tuples(0))

    t1 = System.nanoTime()
    val join_time = (t1 - t0) / 1E9
    println("kNN Join Time: " + join_time + " sec")

    val total_time = leftTime + rightTime + join_time
    //val total_time = 0L
    println("Total Join Time: " + total_time + " sec")
  }
}