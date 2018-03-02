package simba.measurements

import org.apache.spark.sql.simba.SimbaSession
import org.apache.spark.sql.simba.{Dataset, SimbaSession}
import org.apache.spark.sql.simba.{Dataset, SimbaSession}
import org.apache.spark.sql.simba.index.{RTreeType, TreapType}

import org.apache.spark.sql.simba.spatial.Point
import org.apache.spark.sql.simba.spatial.Polygon

object kNNJoin {

  case class PointData(x: Double, y: Double)

  def main(args: Array[String]): Unit = {

    val simbaSession = SimbaSession.builder().appName("Simba kNN Join").config("simba.join.partitions", "50").config("simba.index.partitions", "50").getOrCreate()
    knnJoin(simbaSession)
    simbaSession.stop()
  }

  def knnJoin(simba: SimbaSession) {

    import simba.implicits._
    import simba.simbaImplicits._
    var t0 = 0L
    var t1 = 0L
    t0 = System.nanoTime()
    var DS1 = simba.read.textFile("/data/points_10M.csv").map { line =>
      val parts = line.split(",")
      val longitude = parts(0).toDouble
      val latitude = parts(1).toDouble
      PointData(longitude, latitude)
    }.repartition(1024).toDF()
    var count1 = DS1.count()
    t1 = System.nanoTime()
    var left_time = (t1 - t0) / 1E9
    println("Left Read time: " + left_time + " seconds")

    t0 = System.nanoTime()
    var DS2 = simba.read.textFile("/data/points_10M.csv").map { line =>
      val parts = line.split(",")
      val longitude = parts(0).toDouble
      val latitude = parts(1).toDouble
      PointData(longitude, latitude)
    }.repartition(1024).toDF()
    count1 = DS2.count()
    t1 = System.nanoTime()
    var right_time = (t1 - t0) / 1E9
    println("Right Read time: " + right_time + " seconds")

    //DS2.index(RTreeType, "RtreeForDS2",  Array("x", "y"))

    // Embed timers inside RKJSpark for partitioning time and indexing time
    t0 = System.nanoTime()
    val count = DS1.knnJoin(DS2, Array("x", "y"), Array("x", "y"), 5).count()
    t1 = System.nanoTime()
    val runTime = (t1 - t0) / 1E9
    println("Join time: " + runTime + " seconds")
    println("Total Time: " + (left_time + right_time + runTime) + " seconds")
    println("Count: " + count)
  }
}