package geospark.measurements

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.datasyslab.geospark.enums.FileDataSplitter
import org.datasyslab.geospark.enums.GridType
import org.datasyslab.geospark.enums.IndexType
import org.datasyslab.geospark.spatialOperator.JoinQuery
import org.datasyslab.geospark.spatialOperator.KNNQuery
import org.datasyslab.geospark.spatialOperator.RangeQuery
import org.datasyslab.geospark.spatialRDD.CircleRDD
import org.datasyslab.geospark.spatialRDD.PointRDD
import org.datasyslab.geospark.spatialRDD.PolygonRDD
import org.datasyslab.geospark.spatialRDD.LineStringRDD
import org.datasyslab.geospark.spatialRDD.RectangleRDD
import com.vividsolutions.jts.geom.Coordinate
import com.vividsolutions.jts.geom.Envelope
import com.vividsolutions.jts.geom.GeometryFactory
import org.apache.spark.serializer.KryoSerializer
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileRDD
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import scala.util.Random


/**
  * knnQueries For Different Geometric Objects.
  */

object kNNQueries extends App {

  val conf = new SparkConf().setAppName("GeoSpark kNN Queries")
  conf.set("spark.serializer", classOf[KryoSerializer].getName)
  conf.set("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
  val sc = new SparkContext(conf)
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    println("Query time: " + (t1 - t0) / 1E9 + " sec ")
    result
  }

  val geometryFactory = new GeometryFactory()

  knnPoint()
  //knnLineString()
  //knnPolygon()
  //knnRectangle()

  sc.stop()

  def knnPoint() {

    val nQueries = 100
    val rangeQueryWindow6 = new Envelope(-180.0, 180.0, -90.0, 90.0)
    val random = scala.util.Random

    println("************************ POINT KNN Queries **************************************")

    val objectRDD = new PointRDD(sc, "/data/points_200M.csv", 0, FileDataSplitter.CSV, false, 1024, StorageLevel.MEMORY_ONLY)

    objectRDD.buildIndex(IndexType.RTREE, false)
    objectRDD.indexedRawRDD.persist(StorageLevel.MEMORY_ONLY)

    var t0 = 0L
    var t1 = 0L
    var show = 0L

    // Materialize RDDs
    t0 = System.nanoTime()
    for (i <- 1 to 20) {
      val count1 = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow6, false, true).count()
    }
    t1 = System.nanoTime()

    println("k=1")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      var lat = (random.nextDouble() * 2 - 1) * 90
      var long = (random.nextDouble() * 2 - 1) * 180
      val kNNQueryPoint = geometryFactory.createPoint(new Coordinate(long, lat))
      val count1 = KNNQuery.SpatialKnnQuery(objectRDD, kNNQueryPoint, 1, true) // flag true to use index
    }
    t1 = System.nanoTime()
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    t1 = 0L
    t0 = 0L

    println("k=5")
    for (i <- 1 to nQueries) {
      var lat = (random.nextDouble() * 2 - 1) * 90
      var long = (random.nextDouble() * 2 - 1) * 180
      val kNNQueryPoint = geometryFactory.createPoint(new Coordinate(long, lat))
      val count1 = KNNQuery.SpatialKnnQuery(objectRDD, kNNQueryPoint, 5, true) // flag true to use index
    }
    t1 = System.nanoTime()
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    t1 = 0L
    t0 = 0L

    println("k=10")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      var lat = (random.nextDouble() * 2 - 1) * 90
      var long = (random.nextDouble() * 2 - 1) * 180
      val kNNQueryPoint = geometryFactory.createPoint(new Coordinate(long, lat))
      val count1 = KNNQuery.SpatialKnnQuery(objectRDD, kNNQueryPoint, 10, true) // flag true to use index
    }
    t1 = System.nanoTime()
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    t1 = 0L
    t0 = 0L

    println("k=20")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      var lat = (random.nextDouble() * 2 - 1) * 90
      var long = (random.nextDouble() * 2 - 1) * 180
      val kNNQueryPoint = geometryFactory.createPoint(new Coordinate(long, lat))
      val count1 = KNNQuery.SpatialKnnQuery(objectRDD, kNNQueryPoint, 20, true) // flag true to use index
    }
    t1 = System.nanoTime()
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    t1 = 0L
    t0 = 0L

    println("k=30")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      var lat = (random.nextDouble() * 2 - 1) * 90
      var long = (random.nextDouble() * 2 - 1) * 180
      val kNNQueryPoint = geometryFactory.createPoint(new Coordinate(long, lat))
      val count1 = KNNQuery.SpatialKnnQuery(objectRDD, kNNQueryPoint, 30, true) // flag true to use index
    }
    t1 = System.nanoTime()
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    t1 = 0L
    t0 = 0L

    println("k=40")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      var lat = (random.nextDouble() * 2 - 1) * 90
      var long = (random.nextDouble() * 2 - 1) * 180
      val kNNQueryPoint = geometryFactory.createPoint(new Coordinate(long, lat))
      val count1 = KNNQuery.SpatialKnnQuery(objectRDD, kNNQueryPoint, 40, true) // flag true to use index
    }
    t1 = System.nanoTime()
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    t1 = 0L
    t0 = 0L

    println("k=50")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      var lat = (random.nextDouble() * 2 - 1) * 90
      var long = (random.nextDouble() * 2 - 1) * 180
      val kNNQueryPoint = geometryFactory.createPoint(new Coordinate(long, lat))
      val count1 = KNNQuery.SpatialKnnQuery(objectRDD, kNNQueryPoint, 50, true) // flag true to use index
    }
    t1 = System.nanoTime()
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    t1 = 0L
    t0 = 0L

    objectRDD.indexedRawRDD.unpersist()
    objectRDD.rawSpatialRDD.unpersist()

    println("***********************************************************************************")
    println("")
  }

  def knnLineString() {

    val nQueries = 100
    var tTime = 0L
    val rangeQueryWindow6 = new Envelope(-180.0, 180.0, -90.0, 90.0)
    val random = scala.util.Random

    println("************************ LineString KNN Queries **************************************")

    val objectRDD = new LineStringRDD(sc, "/data/linestrings_72M.csv", FileDataSplitter.WKT, false, 1024, StorageLevel.MEMORY_ONLY)

    objectRDD.buildIndex(IndexType.RTREE, false)
    objectRDD.indexedRawRDD.persist(StorageLevel.MEMORY_ONLY)


    var t0 = 0L
    var t1 = 0L

    t0 = System.nanoTime()
    for (i <- 1 to 20) {
      val count1 = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow6, false, true).count()
    }
    t1 = System.nanoTime()

    println("k=1")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      var lat = (random.nextDouble() * 2 - 1) * 90
      var long = (random.nextDouble() * 2 - 1) * 180
      val kNNQueryPoint = geometryFactory.createPoint(new Coordinate(long, lat))
      val count1 = KNNQuery.SpatialKnnQuery(objectRDD, kNNQueryPoint, 1, true) // flag true to use index
    }
    t1 = System.nanoTime()
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    t1 = 0L
    t0 = 0L

    println("k=5")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      var lat = (random.nextDouble() * 2 - 1) * 90
      var long = (random.nextDouble() * 2 - 1) * 180
      val kNNQueryPoint = geometryFactory.createPoint(new Coordinate(long, lat))
      val count1 = KNNQuery.SpatialKnnQuery(objectRDD, kNNQueryPoint, 5, true) // flag true to use index
    }
    t1 = System.nanoTime()
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    t1 = 0L
    t0 = 0L

    println("k=10")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      var lat = (random.nextDouble() * 2 - 1) * 90
      var long = (random.nextDouble() * 2 - 1) * 180
      val kNNQueryPoint = geometryFactory.createPoint(new Coordinate(long, lat))
      val count1 = KNNQuery.SpatialKnnQuery(objectRDD, kNNQueryPoint, 10, true) // flag true to use index
    }
    t1 = System.nanoTime()
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    t1 = 0L
    t0 = 0L

    println("k=20")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      var lat = (random.nextDouble() * 2 - 1) * 90
      var long = (random.nextDouble() * 2 - 1) * 180
      val kNNQueryPoint = geometryFactory.createPoint(new Coordinate(long, lat))
      val count1 = KNNQuery.SpatialKnnQuery(objectRDD, kNNQueryPoint, 20, true) // flag true to use index
    }
    t1 = System.nanoTime()
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    t1 = 0L
    t0 = 0L

    println("k=30")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      var lat = (random.nextDouble() * 2 - 1) * 90
      var long = (random.nextDouble() * 2 - 1) * 180
      val kNNQueryPoint = geometryFactory.createPoint(new Coordinate(long, lat))
      val count1 = KNNQuery.SpatialKnnQuery(objectRDD, kNNQueryPoint, 30, true) // flag true to use index
    }
    t1 = System.nanoTime()
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    t1 = 0L
    t0 = 0L

    println("k=40")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      var lat = (random.nextDouble() * 2 - 1) * 90
      var long = (random.nextDouble() * 2 - 1) * 180
      val kNNQueryPoint = geometryFactory.createPoint(new Coordinate(long, lat))
      val count1 = KNNQuery.SpatialKnnQuery(objectRDD, kNNQueryPoint, 40, true) // flag true to use index
    }
    t1 = System.nanoTime()
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    t1 = 0L
    t0 = 0L

    println("k=50")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      var lat = (random.nextDouble() * 2 - 1) * 90
      var long = (random.nextDouble() * 2 - 1) * 180
      val kNNQueryPoint = geometryFactory.createPoint(new Coordinate(long, lat))
      val count1 = KNNQuery.SpatialKnnQuery(objectRDD, kNNQueryPoint, 50, true) // flag true to use index
    }
    t1 = System.nanoTime()
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")

    objectRDD.indexedRawRDD.unpersist()
    objectRDD.rawSpatialRDD.unpersist()

    println("***********************************************************************************")
    println("")
  }

  def knnPolygon() {

    val nQueries = 100
    var tTime = 0L
    val rangeQueryWindow6 = new Envelope(-180.0, 180.0, -90.0, 90.0)
    val random = scala.util.Random

    println("************************ POLYGON KNN Queries **************************************")

    val objectRDD = new PolygonRDD(sc, "/data/buildings_114M.csv", 0, 8, FileDataSplitter.WKT, false, 1024, StorageLevel.MEMORY_ONLY)

    objectRDD.buildIndex(IndexType.RTREE, false)
    objectRDD.indexedRawRDD.persist(StorageLevel.MEMORY_ONLY)

    var t0 = 0L
    var t1 = 0L

    t0 = System.nanoTime()
    for (i <- 1 to 20) {
      val count1 = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow6, false, true).count()
    }
    t1 = System.nanoTime()

    println("k=1")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      var lat = (random.nextDouble() * 2 - 1) * 90
      var long = (random.nextDouble() * 2 - 1) * 180
      val kNNQueryPoint = geometryFactory.createPoint(new Coordinate(long, lat))
      val count1 = KNNQuery.SpatialKnnQuery(objectRDD, kNNQueryPoint, 1, true) // flag true to use index
    }
    t1 = System.nanoTime()
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    t1 = 0L
    t0 = 0L

    println("k=5")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      var lat = (random.nextDouble() * 2 - 1) * 90
      var long = (random.nextDouble() * 2 - 1) * 180
      val kNNQueryPoint = geometryFactory.createPoint(new Coordinate(long, lat))
      val count1 = KNNQuery.SpatialKnnQuery(objectRDD, kNNQueryPoint, 5, true) // flag true to use index
    }
    t1 = System.nanoTime()
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    t1 = 0L
    t0 = 0L

    println("k=10")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      var lat = (random.nextDouble() * 2 - 1) * 90
      var long = (random.nextDouble() * 2 - 1) * 180
      val kNNQueryPoint = geometryFactory.createPoint(new Coordinate(long, lat))
      val count1 = KNNQuery.SpatialKnnQuery(objectRDD, kNNQueryPoint, 10, true) // flag true to use index
    }
    t1 = System.nanoTime()
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    t1 = 0L
    t0 = 0L

    println("k=20")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      var lat = (random.nextDouble() * 2 - 1) * 90
      var long = (random.nextDouble() * 2 - 1) * 180
      val kNNQueryPoint = geometryFactory.createPoint(new Coordinate(long, lat))
      val count1 = KNNQuery.SpatialKnnQuery(objectRDD, kNNQueryPoint, 20, true) // flag true to use index
    }
    t1 = System.nanoTime()
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    t1 = 0L
    t0 = 0L

    println("k=30")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      var lat = (random.nextDouble() * 2 - 1) * 90
      var long = (random.nextDouble() * 2 - 1) * 180
      val kNNQueryPoint = geometryFactory.createPoint(new Coordinate(long, lat))
      val count1 = KNNQuery.SpatialKnnQuery(objectRDD, kNNQueryPoint, 30, true) // flag true to use index
    }
    t1 = System.nanoTime()
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    t1 = 0L
    t0 = 0L

    println("k=40")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      var lat = (random.nextDouble() * 2 - 1) * 90
      var long = (random.nextDouble() * 2 - 1) * 180
      val kNNQueryPoint = geometryFactory.createPoint(new Coordinate(long, lat))
      val count1 = KNNQuery.SpatialKnnQuery(objectRDD, kNNQueryPoint, 40, true) // flag true to use index
    }
    t1 = System.nanoTime()
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    t1 = 0L
    t0 = 0L

    println("k=50")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      var lat = (random.nextDouble() * 2 - 1) * 90
      var long = (random.nextDouble() * 2 - 1) * 180
      val kNNQueryPoint = geometryFactory.createPoint(new Coordinate(long, lat))
      val count1 = KNNQuery.SpatialKnnQuery(objectRDD, kNNQueryPoint, 50, true) // flag true to use index
    }
    t1 = System.nanoTime()
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")

    objectRDD.indexedRawRDD.unpersist()
    objectRDD.rawSpatialRDD.unpersist()

    println("***********************************************************************************")
    println("")
  }

  def knnRectangle() {

    val nQueries = 100
    var tTime = 0L
    val rangeQueryWindow6 = new Envelope(-180.0, 180.0, -90.0, 90.0)
    val random = scala.util.Random

    println("************************ Rectangle KNN Queries **************************************")

    val objectRDD = new RectangleRDD(sc, "/data/rectangles_114M.csv", FileDataSplitter.WKT, false, 1024, StorageLevel.MEMORY_ONLY)

    objectRDD.buildIndex(IndexType.RTREE, false)
    objectRDD.indexedRawRDD.persist(StorageLevel.MEMORY_ONLY)

    var t0 = 0L
    var t1 = 0L

    t0 = System.nanoTime()
    for (i <- 1 to 20) {
      val count1 = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow6, false, true).count()
    }
    t1 = System.nanoTime()

    println("k=1")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      var lat = (random.nextDouble() * 2 - 1) * 90
      var long = (random.nextDouble() * 2 - 1) * 180
      val kNNQueryPoint = geometryFactory.createPoint(new Coordinate(long, lat))
      val count1 = KNNQuery.SpatialKnnQuery(objectRDD, kNNQueryPoint, 1, true) // flag true to use index
    }
    t1 = System.nanoTime()
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    t1 = 0L
    t0 = 0L

    println("k=5")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      var lat = (random.nextDouble() * 2 - 1) * 90
      var long = (random.nextDouble() * 2 - 1) * 180
      val kNNQueryPoint = geometryFactory.createPoint(new Coordinate(long, lat))
      val count1 = KNNQuery.SpatialKnnQuery(objectRDD, kNNQueryPoint, 5, true) // flag true to use index
    }
    t1 = System.nanoTime()
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    t1 = 0L
    t0 = 0L

    println("k=10")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      var lat = (random.nextDouble() * 2 - 1) * 90
      var long = (random.nextDouble() * 2 - 1) * 180
      val kNNQueryPoint = geometryFactory.createPoint(new Coordinate(long, lat))
      val count1 = KNNQuery.SpatialKnnQuery(objectRDD, kNNQueryPoint, 10, true) // flag true to use index
    }
    t1 = System.nanoTime()
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    t1 = 0L
    t0 = 0L

    println("k=20")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      var lat = (random.nextDouble() * 2 - 1) * 90
      var long = (random.nextDouble() * 2 - 1) * 180
      val kNNQueryPoint = geometryFactory.createPoint(new Coordinate(long, lat))
      val count1 = KNNQuery.SpatialKnnQuery(objectRDD, kNNQueryPoint, 20, true) // flag true to use index
    }
    t1 = System.nanoTime()
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    t1 = 0L
    t0 = 0L

    println("k=30")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      var lat = (random.nextDouble() * 2 - 1) * 90
      var long = (random.nextDouble() * 2 - 1) * 180
      val kNNQueryPoint = geometryFactory.createPoint(new Coordinate(long, lat))
      val count1 = KNNQuery.SpatialKnnQuery(objectRDD, kNNQueryPoint, 30, true) // flag true to use index
    }
    t1 = System.nanoTime()
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    t1 = 0L
    t0 = 0L

    println("k=40")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      var lat = (random.nextDouble() * 2 - 1) * 90
      var long = (random.nextDouble() * 2 - 1) * 180
      val kNNQueryPoint = geometryFactory.createPoint(new Coordinate(long, lat))
      val count1 = KNNQuery.SpatialKnnQuery(objectRDD, kNNQueryPoint, 40, true) // flag true to use index
    }
    t1 = System.nanoTime()
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    t1 = 0L
    t0 = 0L

    println("k=50")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      var lat = (random.nextDouble() * 2 - 1) * 90
      var long = (random.nextDouble() * 2 - 1) * 180
      val kNNQueryPoint = geometryFactory.createPoint(new Coordinate(long, lat))
      val count1 = KNNQuery.SpatialKnnQuery(objectRDD, kNNQueryPoint, 50, true) // flag true to use index
    }
    t1 = System.nanoTime()
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")

    objectRDD.indexedRawRDD.unpersist()
    objectRDD.rawSpatialRDD.unpersist()

    println("***********************************************************************************")
    println("")
  }
}
