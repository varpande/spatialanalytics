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
import scala.util.Random
import org.apache.spark.{SparkConf, SparkContext}

object kNNQueries {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("LocationSpark kNN Queries")
    val sc = new SparkContext(conf)
    sc.setLogLevel("OFF")
    Util.localIndex = "QTREE"
    val input = "/data/points_200M_wkt.csv"
    val random = scala.util.Random

    println("************************************** LocationSpark kNN Queries **************************************************")

    val rangeQueryWindow1 = Box(-50.3010141441f, -53.209588996f, -24.9526465797f, -30.1096863746f)
    val rangeQueryWindow2 = Box(-54.4270741441f, -53.209588996f, -24.9526465797f, -30.1096863746f)
    val rangeQueryWindow3 = Box(-114.4270741441f, -54.509588996f, 42.9526465797f, -27.0106863746f)
    val rangeQueryWindow4 = Box(-82.7638020000f, -54.509588996f, 42.9526465797f, 38.0106863746f)
    val rangeQueryWindow5 = Box(-140.99778f, -52.6480987209f, 5.7305630159f, 83.23324f)
    val rangeQueryWindow6 = Box(-180.0f, -90.0f, 180.0f, 90.0f)

    val nQueries = 100
    var t0 = 0L
    var t1 = 0L

    t0 = System.nanoTime()
    val leftpoints = sc.textFile("/data/points_200M_wkt.csv", 1024).map(x => (Try(new WKTReader().read(x)))).filter(_.isSuccess).map {
      case x =>
        val corrds = x.get.getCoordinates
        val p1 = corrds(0)
        (Point(p1.x.toFloat, p1.y.toFloat), "1")
    }
    val leftLocationRDD = SpatialRDD(leftpoints).cache()
    val size = leftLocationRDD.count()
    t1 = System.nanoTime()
    var read_time = ((t1 - t0) / 1E9)
    println("Left Indexing Time: " + read_time + " sec")
    var knnresults = leftLocationRDD.knnFilter(Point(107.32653223f, 44.323999f), 5, (id) => true)
    knnresults.size

    t1 = 0L
    t0 = 0L

    println("k=10")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      var lat = (random.nextFloat() * 2 - 1) * 90
      var long = (random.nextFloat() * 2 - 1) * 180
      var qPoint = Point(long, lat)
      var count1 = leftLocationRDD.knnFilter(qPoint, 10, (id) => true)
      val count = count1.size
    }
    t1 = System.nanoTime()
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    t1 = 0L
    t0 = 0L

    println("k=20")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      var lat = (random.nextFloat() * 2 - 1) * 90
      var long = (random.nextFloat() * 2 - 1) * 180
      var qPoint = Point(long, lat)
      var count1 = leftLocationRDD.knnFilter(qPoint, 20, (id) => true)
      val count = count1.size
    }
    t1 = System.nanoTime()
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    t1 = 0L
    t0 = 0L

    println("k=30")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      var lat = (random.nextFloat() * 2 - 1) * 90
      var long = (random.nextFloat() * 2 - 1) * 180
      var qPoint = Point(long, lat)
      var count1 = leftLocationRDD.knnFilter(qPoint, 30, (id) => true)
      val count = count1.size
    }
    t1 = System.nanoTime()
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    t1 = 0L
    t0 = 0L

    println("k=40")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      var lat = (random.nextFloat() * 2 - 1) * 90
      var long = (random.nextFloat() * 2 - 1) * 180
      var qPoint = Point(long, lat)
      var count1 = leftLocationRDD.knnFilter(qPoint, 40, (id) => true)
      val count = count1.size
    }
    t1 = System.nanoTime()
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    t1 = 0L
    t0 = 0L

    println("k=50")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      var lat = (random.nextFloat() * 2 - 1) * 90
      var long = (random.nextFloat() * 2 - 1) * 180
      var qPoint = Point(long, lat)
      var count1 = leftLocationRDD.knnFilter(qPoint, 50, (id) => true)
      val count = count1.size
    }
    t1 = System.nanoTime()
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    t1 = 0L
    t0 = 0L

    println("****************************************************************************************")
    println()

  }
}