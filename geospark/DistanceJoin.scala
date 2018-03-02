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
  * Distance Join For Points.
  */

object DistanceJoin extends App {

  val conf = new SparkConf().setAppName("GeoSpark Distance Join")
  conf.set("spark.serializer", classOf[KryoSerializer].getName)
  conf.set("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
  val sc = new SparkContext(conf)
  Logger.getLogger("org").setLevel(Level.INFO)
  Logger.getLogger("akka").setLevel(Level.INFO)

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    println("Join time: " + (t1 - t0) / 1E9 + " sec ")
    result
  }

  val geometryFactory = new GeometryFactory()

  distanceJoin()

  sc.stop()

  def distanceJoin() {

    var t0 = 0L
    var t1 = 0L
    var count1 = 0L

    println("************************ POINTS Distance Join **************************************")
    t0 = System.nanoTime()
    val objectRDD = new PointRDD(sc, "/data/points_200M.csv", 0, FileDataSplitter.CSV, false, 1024, StorageLevel.MEMORY_ONLY)
    t1 = System.nanoTime()
    val read_time = ((t1 - t0) / 1E9)

    println("Read Time: " + read_time + " sec")

    t0 = System.nanoTime()

    objectRDD.spatialPartitioning(GridType.QUADTREE)

    objectRDD.buildIndex(IndexType.RTREE, true)

    objectRDD.indexedRDD.persist(StorageLevel.MEMORY_ONLY)

    objectRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)

    val c1 = objectRDD.spatialPartitionedRDD.count()

    val c2 = objectRDD.indexedRDD.count()

    t1 = System.nanoTime()
    val left_time = ((t1 - t0) / 1E9)
    println("Left Partitioning and Indexing Time: " + left_time + " sec")

    objectRDD.rawSpatialRDD.unpersist()

    t0 = System.nanoTime()

    val queryWindow = new CircleRDD(objectRDD, 0.000045027) // Simba computes euclidean distance, this distance is in degrees (in meters it would be approx 5 meters considering worst case scenario where 1 degree is equal to 110 kms at the equator)

    queryWindow.spatialPartitioning(objectRDD.getPartitioner)

    queryWindow.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)

    val c3 = queryWindow.spatialPartitionedRDD.count()

    t1 = System.nanoTime()
    val right_time = ((t1 - t0) / 1E9)
    println("Right Partitioning and Indexing Time: " + right_time + " sec")

    queryWindow.rawSpatialRDD.unpersist()

    t0 = System.nanoTime()
    count1 = time(JoinQuery.DistanceJoinQuery(objectRDD, queryWindow, true, false).count())
    t1 = System.nanoTime()
    val join_time = ((t1 - t0) / 1E9)
    println("Distance Join Runtime: " + join_time + " sec")

    val total_time = read_time + left_time + right_time + join_time

    println("Total Runtime: " + total_time + " sec")

    println("***********************************************************************************")
    println("")
  }

}
