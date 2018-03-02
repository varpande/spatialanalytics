package simba.measurements

import org.apache.spark.sql.simba.SimbaSession
import org.apache.spark.sql.simba.{Dataset, SimbaSession}
import org.apache.spark.sql.simba.index.{RTreeType, TreapType}
import org.apache.spark.sql.simba.spatial.Point
import org.apache.spark.sql.simba.spatial.Polygon
import scala.util.Random

object kNNQueries {

  case class PointData(x: Double, y: Double)

  def main(args: Array[String]): Unit = {

    val simbaSession = SimbaSession.builder().appName("Simba kNN Queries").config("simba.index.partitions", "1024").getOrCreate()
    knn_queries(simbaSession)
    simbaSession.stop()
  }

  def knn_queries(simba: SimbaSession) {

    import simba.implicits._
    import simba.simbaImplicits._

    val nQueries = 100
    var t0 = 0L
    var t1 = 0L
    var count1 = 0L
    val random = scala.util.Random

    t0 = System.nanoTime()
    var DS1 = simba.read.textFile("/data/points_200M.csv").map { line =>
      val parts = line.split(",")
      val longitude = parts(0).toDouble
      val latitude = parts(1).toDouble
      PointData(longitude, latitude)
    }.repartition(1024).toDF()
    DS1.index(RTreeType, "RtreeForDS1", Array("x", "y"))
    var count = DS1.count()
    t1 = System.nanoTime()
    var left_time = (t1 - t0) / 1E9
    println("Read time: " + (left_time) + " seconds")
    println()

    // Dry run
    println("k=5")
    t0 = System.nanoTime()
    for (i <- 1 to 20) {
      var lat = (random.nextDouble() * 2 - 1) * 90
      var long = (random.nextDouble() * 2 - 1) * 180
      count1 = DS1.knn(Array("x", "y"), Array(long, lat), 5).count()
    }
    t1 = System.nanoTime()

    // Actual Measurements
    println("k=5")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      var lat = (random.nextDouble() * 2 - 1) * 90
      var long = (random.nextDouble() * 2 - 1) * 180
      count1 = DS1.knn(Array("x", "y"), Array(long, lat), 5).count()
    }
    t1 = System.nanoTime()
    println("Count: " + count1)
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")

    println("k=10")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      var lat = (random.nextDouble() * 2 - 1) * 90
      var long = (random.nextDouble() * 2 - 1) * 180
      count1 = DS1.knn(Array("x", "y"), Array(long, lat), 10).count()
    }
    t1 = System.nanoTime()
    println("Count: " + count1)
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")

    println("k=20")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      var lat = (random.nextDouble() * 2 - 1) * 90
      var long = (random.nextDouble() * 2 - 1) * 180
      count1 = DS1.knn(Array("x", "y"), Array(long, lat), 20).count()
    }
    t1 = System.nanoTime()
    println("Count: " + count1)
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")

    println("k=30")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      var lat = (random.nextDouble() * 2 - 1) * 90
      var long = (random.nextDouble() * 2 - 1) * 180
      count1 = DS1.knn(Array("x", "y"), Array(long, lat), 30).count()
    }
    t1 = System.nanoTime()
    println("Count: " + count1)
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")

    println("k=40")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      var lat = (random.nextDouble() * 2 - 1) * 90
      var long = (random.nextDouble() * 2 - 1) * 180
      count1 = DS1.knn(Array("x", "y"), Array(long, lat), 40).count()
    }
    t1 = System.nanoTime()
    println("Count: " + count1)
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")

    println("k=50")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      var lat = (random.nextDouble() * 2 - 1) * 90
      var long = (random.nextDouble() * 2 - 1) * 180
      count1 = DS1.knn(Array("x", "y"), Array(long, lat), 50).count()
    }
    t1 = System.nanoTime()
    println("Count: " + count1)
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")

  }
}
