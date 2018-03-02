package simba.measurements

import org.apache.spark.sql.simba.SimbaSession
import org.apache.spark.sql.simba.{Dataset, SimbaSession}
import org.apache.spark.sql.simba.{Dataset, SimbaSession}
import org.apache.spark.sql.simba.index.{RTreeType, TreapType}

import org.apache.spark.sql.simba.spatial.Point
import org.apache.spark.sql.simba.spatial.Polygon

object RangeQueries {

  case class PointData(x: Double, y: Double)

  def main(args: Array[String]): Unit = {

    val simbaSession = SimbaSession.builder().appName("Simba Range Queries").config("simba.index.partitions", "1024").getOrCreate()

    range_queries(simbaSession)
    simbaSession.stop()
  }

  def range_queries(simba: SimbaSession) {

    import simba.implicits._
    import simba.simbaImplicits._

    var t0 = 0L
    var t1 = 0L
    val nQueries = 100
    var count1 = 0L

    t0 = System.nanoTime()
    var DS1 = simba.read.textFile("/data/points_200M.csv").map { line =>
      val parts = line.split(",")
      val pickup_longitude = parts(0).toDouble
      val pickup_latitude = parts(1).toDouble
      PointData(pickup_longitude, pickup_latitude)
    }.repartition(1024).toDF()
    DS1.index(RTreeType, "RtreeForDS1", Array("x", "y"))
    var count = DS1.count()
    t1 = System.nanoTime()
    var left_time = (t1 - t0) / 1E9
    println("Read time: " + left_time + " seconds")
    println()

    // Dry run
    t0 = System.nanoTime()
    for (i <- 1 to 20) {
      count1 = DS1.range(Array("x", "y"), Array(-50.3010141441, -53.209588996), Array(-24.9526465797, -30.1096863746)).rdd.count()
    }
    t1 = System.nanoTime()

    // Actual Measurements
    println("Range1: ")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      count1 = DS1.range(Array("x", "y"), Array(-50.3010141441, -53.209588996), Array(-24.9526465797, -30.1096863746)).rdd.count()
    }
    t1 = System.nanoTime()
    println("Count: " + count1)
    println("Selection Ratio: " + ((count1 * 100.0) / count))
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")

    println("Range2: ")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      count1 = DS1.range(Array("x", "y"), Array(-54.4270741441, -53.209588996), Array(-24.9526465797, -30.1096863746)).rdd.count()
    }
    t1 = System.nanoTime()
    println("Count: " + count1)
    println("Selection Ratio: " + ((count1 * 100.0) / count))
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")

    println("Range3: ")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      count1 = DS1.range(Array("x", "y"), Array(-114.4270741441, -54.509588996), Array(42.9526465797, -27.0106863746)).rdd.count()
    }
    t1 = System.nanoTime()
    println("Count: " + count1)
    println("Selection Ratio: " + ((count1 * 100.0) / count))
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")

    println("Range4: ")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      count1 = DS1.range(Array("x", "y"), Array(-82.7638020000, -54.509588996), Array(42.9526465797, 38.0106863746)).rdd.count()
    }
    t1 = System.nanoTime()
    println("Count: " + count1)
    println("Selection Ratio: " + ((count1 * 100.0) / count))
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")

    println("Range5: ")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      count1 = DS1.range(Array("x", "y"), Array(-140.99778, -52.6480987209), Array(5.7305630159, 83.23324)).rdd.count()
    }
    t1 = System.nanoTime()
    println("Count: " + count1)
    println("Selection Ratio: " + ((count1 * 100.0) / count))
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")

    println("Range6: ")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      count1 = DS1.range(Array("x", "y"), Array(-180.0, -90.0), Array(180.0, 90.0)).rdd.count()
    }
    t1 = System.nanoTime()
    println("Count: " + count1)
    println("Selection Ratio: " + ((count1 * 100.0) / count))
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")

  }
}
