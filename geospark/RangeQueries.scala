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


/**
  * Range Queries For Different Geometric Objects.
  */

/**
  * rangeQueryWindow1 ==> Selection ratio 0.0001
  * rangeQueryWindow2 ==> Selection ratio 0.01
  * rangeQueryWindow3 ==> Selection ratio 1.0
  * rangeQueryWindow4 ==> Selection ratio 10.0
  * rangeQueryWindow5 ==> Selection ratio 50.0
  * rangeQueryWindow6 ==> Selection ratio 100.0
  */

object RangeQueries extends App {

  val conf = new SparkConf().setAppName("GeoSpark Range Queries")
  conf.set("spark.serializer", classOf[KryoSerializer].getName)
  conf.set("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
  val sc = new SparkContext(conf)
  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)

  val geometryFactory = new GeometryFactory()

  spatialRangePoint()
  spatialRangeLineString()
  spatialRangePolygon()
  spatialRangeRectangle()

  sc.stop()

  def spatialRangePoint() {

    val nQueries = 100
    val rangeQueryWindow1 = new Envelope(-50.3010141441, -24.9526465797, -53.209588996, -30.1096863746)
    val rangeQueryWindow2 = new Envelope(-54.4270741441, -24.9526465797, -53.209588996, -30.1096863746)
    val rangeQueryWindow3 = new Envelope(-114.4270741441, 42.9526465797, -54.509588996, -27.0106863746)
    val rangeQueryWindow4 = new Envelope(-82.7638020000, 42.9526465797, -54.509588996, 38.0106863746)
    val rangeQueryWindow5 = new Envelope(-140.99778, 5.7305630159, -52.6480987209, 83.23324)
    val rangeQueryWindow6 = new Envelope(-180.0, 180.0, -90.0, 90.0)

    println("************************ POINT Range Queries **************************************")

    val objectRDD = new PointRDD(sc, "/data/points_200M.csv", 0, FileDataSplitter.CSV, false, 1024, StorageLevel.MEMORY_ONLY)

    objectRDD.buildIndex(IndexType.RTREE, false)
    objectRDD.indexedRawRDD.persist(StorageLevel.MEMORY_ONLY)

    var t0 = 0L
    var t1 = 0L
    var count1 = 0L
    var count = 0L

    // Materialize IndexedRDD
    t0 = System.nanoTime()
    for (i <- 1 to 20) {
      count = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow6, false, true).count()
    }
    t1 = System.nanoTime()

    // Actual Measurements
    println("Range1: ")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      count1 = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow1, false, true).count()
    }
    t1 = System.nanoTime()
    println("Count: " + count1)
    println("Selection Ratio: " + ((count1 * 100.0) / count))
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    t1 = 0L
    t0 = 0L

    println("Range2: ")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      count1 = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow2, false, true).count()
    }
    t1 = System.nanoTime()
    println("Count: " + count1)
    println("Selection Ratio: " + ((count1 * 100.0) / count))
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    t1 = 0L
    t0 = 0L

    println("Range3: ")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      count1 = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow3, false, true).count()
    }
    t1 = System.nanoTime()
    println("Count: " + count1)
    println("Selection Ratio: " + ((count1 * 100.0) / count))
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    t1 = 0L
    t0 = 0L

    println("Range4: ")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      count1 = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow4, false, true).count()
    }
    t1 = System.nanoTime()
    println("Count: " + count1)
    println("Selection Ratio: " + ((count1 * 100.0) / count))
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    t1 = 0L
    t0 = 0L

    println("Range5: ")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      count1 = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow5, false, true).count()
    }
    t1 = System.nanoTime()
    println("Count: " + count1)
    println("Selection Ratio: " + ((count1 * 100.0) / count))
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    t1 = 0L
    t0 = 0L

    println("Range6: ")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      count1 = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow6, false, true).count()
    }
    t1 = System.nanoTime()
    println("Count: " + count1)
    println("Selection Ratio: " + ((count1 * 100.0) / count))
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    t1 = 0L
    t0 = 0L

    objectRDD.indexedRawRDD.unpersist()
    objectRDD.rawSpatialRDD.unpersist()

    println("***********************************************************************************")
    println("")
  }

  def spatialRangeLineString() {

    val nQueries = 100
    val rangeQueryWindow1 = new Envelope(-50.204, -24.9526465797, -53.209588996, -30.1096863746)
    val rangeQueryWindow2 = new Envelope(-52.1270741441, -24.9526465797, -53.209588996, -30.1096863746)
    val rangeQueryWindow3 = new Envelope(-94.4270741441, 22.9526465797, -34.609588996, -27.0106863746)
    val rangeQueryWindow4 = new Envelope(-74.0938020000, 42.9526465797, -54.509588996, 38.0106863746)
    val rangeQueryWindow5 = new Envelope(-150.99778, 7.2705630159, -52.6480987209, 83.23324)
    val rangeQueryWindow6 = new Envelope(-180.0, 180.0, -90.0, 90.0)

    println("************************ LineString Range Queries **************************************")

    val objectRDD = new LineStringRDD(sc, "/data/linestrings_72M.csv", FileDataSplitter.WKT, false, 1024, StorageLevel.MEMORY_ONLY)

    objectRDD.buildIndex(IndexType.RTREE, false)
    objectRDD.indexedRawRDD.persist(StorageLevel.MEMORY_ONLY)

    var t0 = 0L
    var t1 = 0L
    var count1 = 0L
    var count = 0L

    // Materialize RDDs
    t0 = System.nanoTime()
    for (i <- 1 to 20) {
      count = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow6, false, true).count()
    }
    t1 = System.nanoTime()

    // Actual Measurements
    println("Range1: ")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      count1 = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow1, false, true).count()
    }
    t1 = System.nanoTime()
    println("Count: " + count1)
    println("Selection Ratio: " + ((count1 * 100.0) / count))
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    t1 = 0L
    t0 = 0L

    println("Range2: ")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      count1 = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow2, false, true).count()
    }
    t1 = System.nanoTime()
    println("Count: " + count1)
    println("Selection Ratio: " + ((count1 * 100.0) / count))
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    t1 = 0L
    t0 = 0L

    println("Range3: ")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      count1 = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow3, false, true).count()
    }
    t1 = System.nanoTime()
    println("Count: " + count1)
    println("Selection Ratio: " + ((count1 * 100.0) / count))
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    t1 = 0L
    t0 = 0L

    println("Range4: ")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      count1 = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow4, false, true).count()
    }
    t1 = System.nanoTime()
    println("Count: " + count1)
    println("Selection Ratio: " + ((count1 * 100.0) / count))
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    t1 = 0L
    t0 = 0L

    println("Range5: ")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      count1 = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow5, false, true).count()
    }
    t1 = System.nanoTime()
    println("Count: " + count1)
    println("Selection Ratio: " + ((count1 * 100.0) / count))
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    t1 = 0L
    t0 = 0L

    println("Range6: ")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      count1 = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow6, false, true).count()
    }
    t1 = System.nanoTime()
    println("Count: " + count1)
    println("Selection Ratio: " + ((count1 * 100.0) / count))
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    t1 = 0L
    t0 = 0L

    objectRDD.indexedRawRDD.unpersist()
    objectRDD.rawSpatialRDD.unpersist()

    println("***********************************************************************************")
    println("")
  }

  def spatialRangePolygon() {

    val nQueries = 100
    val rangeQueryWindow1 = new Envelope(-20.204, 17.9526465797, -53.209588996, -30.1096863746)
    val rangeQueryWindow2 = new Envelope(-20.204, 20.4376465797, -53.209588996, -30.1096863746)
    val rangeQueryWindow3 = new Envelope(-74.4270741441, 72.9526465797, -34.609588996, -6.5906863746)
    val rangeQueryWindow4 = new Envelope(-104.0938020000, 118.9526465797, -54.509588996, 40.2406863746)
    val rangeQueryWindow5 = new Envelope(-174.4270741441, 72.9526465797, -34.609588996, 48.4396863746)
    val rangeQueryWindow6 = new Envelope(-180.0, 180.0, -90.0, 90.0)

    println("************************ POLYGON Range Queries **************************************")

    val objectRDD = new PolygonRDD(sc, "/data/buildings_114M.csv", 0, 8, FileDataSplitter.WKT, false, 1024, StorageLevel.MEMORY_ONLY)

    objectRDD.buildIndex(IndexType.RTREE, false)
    objectRDD.indexedRawRDD.persist(StorageLevel.MEMORY_ONLY)

    var t0 = 0L
    var t1 = 0L
    var count1 = 0L
    var count = 0L

    // Materialize RDDs
    t0 = System.nanoTime()
    for (i <- 1 to 20) {
      count = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow6, false, true).count()
    }
    t1 = System.nanoTime()

    // Actual Measurements
    println("Range1: ")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      count1 = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow1, false, true).count()
    }
    t1 = System.nanoTime()
    println("Count: " + count1)
    println("Selection Ratio: " + ((count1 * 100.0) / count))
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    t1 = 0L
    t0 = 0L

    println("Range2: ")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      count1 = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow2, false, true).count()
    }
    t1 = System.nanoTime()
    println("Count: " + count1)
    println("Selection Ratio: " + ((count1 * 100.0) / count))
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    t1 = 0L
    t0 = 0L

    println("Range3: ")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      count1 = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow3, false, true).count()
    }
    t1 = System.nanoTime()
    println("Count: " + count1)
    println("Selection Ratio: " + ((count1 * 100.0) / count))
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    t1 = 0L
    t0 = 0L

    println("Range4: ")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      count1 = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow4, false, true).count()
    }
    t1 = System.nanoTime()
    println("Count: " + count1)
    println("Selection Ratio: " + ((count1 * 100.0) / count))
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    t1 = 0L
    t0 = 0L

    println("Range5: ")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      count1 = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow5, false, true).count()
    }
    t1 = System.nanoTime()
    println("Count: " + count1)
    println("Selection Ratio: " + ((count1 * 100.0) / count))
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    t1 = 0L
    t0 = 0L

    println("Range6: ")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      count1 = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow6, false, true).count()
    }
    t1 = System.nanoTime()
    println("Count: " + count1)
    println("Selection Ratio: " + ((count1 * 100.0) / count))
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    t1 = 0L
    t0 = 0L

    objectRDD.indexedRawRDD.unpersist()
    objectRDD.rawSpatialRDD.unpersist()

    println("***********************************************************************************")
    println("")
  }

  def spatialRangeRectangle() {

    val nQueries = 100
    val rangeQueryWindow1 = new Envelope(-20.204, 17.9526465797, -53.209588996, -30.1096863746)
    val rangeQueryWindow2 = new Envelope(-20.204, 20.4376465797, -53.209588996, -30.1096863746)
    val rangeQueryWindow3 = new Envelope(-74.4270741441, 72.9526465797, -34.609588996, -6.5906863746)
    val rangeQueryWindow4 = new Envelope(-104.0938020000, 118.9526465797, -54.509588996, 40.2406863746)
    val rangeQueryWindow5 = new Envelope(-174.4270741441, 72.9526465797, -34.609588996, 48.4396863746)
    val rangeQueryWindow6 = new Envelope(-180.0, 180.0, -90.0, 90.0)

    println("************************ Rectangle Range Queries **************************************")

    val objectRDD = new RectangleRDD(sc, "/data/rectangles_114M.csv", FileDataSplitter.WKT, false, 1024, StorageLevel.MEMORY_ONLY)

    objectRDD.buildIndex(IndexType.RTREE, false)
    objectRDD.indexedRawRDD.persist(StorageLevel.MEMORY_ONLY)

    var t0 = 0L
    var t1 = 0L
    var count1 = 0L
    var count = 0L
    t0 = System.nanoTime()

    // Materialize RDDs
    for (i <- 1 to 20) {
      count = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow6, false, true).count()
    }
    t1 = System.nanoTime()

    // Actual Measurements
    println("Range1: ")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      count1 = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow1, false, true).count()
    }
    t1 = System.nanoTime()
    println("Count: " + count1)
    println("Selection Ratio: " + ((count1 * 100.0) / count))
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    t1 = 0L
    t0 = 0L

    println("Range2: ")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      count1 = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow2, false, true).count()
    }
    t1 = System.nanoTime()
    println("Count: " + count1)
    println("Selection Ratio: " + ((count1 * 100.0) / count))
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    t1 = 0L
    t0 = 0L

    println("Range3: ")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      count1 = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow3, false, true).count()
    }
    t1 = System.nanoTime()
    println("Count: " + count1)
    println("Selection Ratio: " + ((count1 * 100.0) / count))
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    t1 = 0L
    t0 = 0L

    println("Range4: ")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      count1 = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow4, false, true).count()
    }
    t1 = System.nanoTime()
    println("Count: " + count1)
    println("Selection Ratio: " + ((count1 * 100.0) / count))
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    t1 = 0L
    t0 = 0L

    println("Range5: ")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      count1 = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow5, false, true).count()
    }
    t1 = System.nanoTime()
    println("Count: " + count1)
    println("Selection Ratio: " + ((count1 * 100.0) / count))
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    t1 = 0L
    t0 = 0L

    println("Range6: ")
    t0 = System.nanoTime()
    for (i <- 1 to nQueries) {
      count1 = RangeQuery.SpatialRangeQuery(objectRDD, rangeQueryWindow6, false, true).count()
    }
    t1 = System.nanoTime()
    println("Count: " + count1)
    println("Selection Ratio: " + ((count1 * 100.0) / count))
    println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
    println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
    t1 = 0L
    t0 = 0L

    objectRDD.indexedRawRDD.unpersist()
    objectRDD.rawSpatialRDD.unpersist()

    println("***********************************************************************************")
    println("")
  }
}
