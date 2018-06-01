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
import org.apache.spark.{SparkConf, SparkContext}

object RangeQueries {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("LocationSpark Range Queries")
    val sc = new SparkContext(conf)
    sc.setLogLevel("OFF")
    Util.localIndex = "QTREE"
    val input = "/data/points_200M_wkt.csv"

    val rangeQueryWindow1 = Box(-50.3010141441f, -53.209588996f, -24.9526465797f, -30.1096863746f)
    val rangeQueryWindow2 = Box(-54.4270741441f, -53.209588996f, -24.9526465797f, -30.1096863746f)
    val rangeQueryWindow3 = Box(-114.4270741441f, -54.509588996f, 42.9526465797f, -27.0106863746f)
    val rangeQueryWindow4 = Box(-82.7638020000f, -54.509588996f, 42.9526465797f, 38.0106863746f)
    val rangeQueryWindow5 = Box(-140.99778f, -52.6480987209f, 5.7305630159f, 83.23324f)
    val rangeQueryWindow6 = Box(-180.0f, -90.0f, 180.0f, 90.0f)

    val nQueries = 100
    var t0 = 0L
    var t1 = 0L

    val leftpoints = sc.textFile("/data/points_200M_wkt.csv", 1024).map(x => (Try(new WKTReader().read(x)))).filter(_.isSuccess).map {
      case x =>
        val corrds = x.get.getCoordinates
        val p1 = corrds(0)
        (Point(p1.x.toFloat, p1.y.toFloat), "1")
    }

    val leftLocationRDD = SpatialRDD(leftpoints).cache()
    val count = leftLocationRDD.count()

    println("Range1: 0.00001 %")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      var count1 = leftLocationRDD.rangeFilter(rangeQueryWindow1, (id) => true)
    }
    t1 = System.nanoTime()
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")

    println("Range2: 0.001 %")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      var count1 = leftLocationRDD.rangeFilter(rangeQueryWindow2, (id) => true)
    }
    t1 = System.nanoTime()
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")

    println("Range3: 1.0 %")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      var count1 = leftLocationRDD.rangeFilter(rangeQueryWindow3, (id) => true)
    }
    t1 = System.nanoTime()
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")

    println("Range4: 10.0 %")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      var count1 = leftLocationRDD.rangeFilter(rangeQueryWindow4, (id) => true)
    }
    t1 = System.nanoTime()
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")

    println("Range5: 50.0 %")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      var count1 = leftLocationRDD.rangeFilter(rangeQueryWindow5, (id) => true)
    }
    t1 = System.nanoTime()
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")

    println("Range6: 100.0 %")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      var count1 = leftLocationRDD.rangeFilter(rangeQueryWindow6, (id) => true)
    }
    t1 = System.nanoTime()
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")

  }
}
