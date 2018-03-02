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
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileRDD
import org.apache.spark.serializer.KryoSerializer
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator

/*
 * Spatial Joins between different geometric objects using KDB and Quadtree partitioning
 */

object SpatialJoins {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("GeoSpark Spatial Joins")
    conf.set("spark.serializer", classOf[KryoSerializer].getName)
    conf.set("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
    val sc = new SparkContext(conf)
    Logger.getLogger("org").setLevel(Level.INFO)
    Logger.getLogger("akka").setLevel(Level.INFO)

    val points = "/data/points_200M.csv"
    val polygons = "/data/buildings_114M.csv"
    val rectangles = "/data/rectangles_114M.csv"
    val linestrings = "/data/linestrings_72M.csv"

    val quad = GridType.QUADTREE
    val kdb = GridType.KDBTREE
    val idx = IndexType.RTREE
    val numPartitions = 1024

    /*println("******************************* KDB Partitioning *******************************")
    runSpatialJoin(kdb,"point","point")
    runSpatialJoin(kdb,"point","linestring")
    runSpatialJoin(kdb,"point","polygon")
    runSpatialJoin(kdb,"point","rectangle")
    runSpatialJoin(kdb,"linestring","linestring")
    runSpatialJoin(kdb,"linestring","polygon")
    runSpatialJoin(kdb,"linestring","rectangle")
    runSpatialJoin(kdb,"rectangle","rectangle")
    runSpatialJoin(kdb,"rectangle","polygon")
    runSpatialJoin(kdb,"polygon","polygon")
    println("*************************** Finished KDB Partitioning ***************************")*/
    println("******************************* quad Partitioning *******************************")
    runSpatialJoin(quad, "point", "point")
    runSpatialJoin(quad, "point", "linestring")
    runSpatialJoin(quad, "point", "polygon")
    runSpatialJoin(quad, "point", "rectangle")
    runSpatialJoin(quad, "linestring", "linestring")
    runSpatialJoin(quad, "linestring", "polygon")
    runSpatialJoin(quad, "linestring", "rectangle")
    runSpatialJoin(quad, "rectangle", "rectangle")
    runSpatialJoin(quad, "rectangle", "polygon")
    runSpatialJoin(quad, "polygon", "polygon")
    println("*************************** Finished quad Partitioning ***************************")

    def runSpatialJoin(partitioningScheme: org.datasyslab.geospark.enums.GridType, leftrdd: String, rightrdd: String) {

      var count = 0L
      val beginTime = System.currentTimeMillis()
      var t0 = 0L
      var t1 = 0L

      println("******************************** " + leftrdd + " and " + rightrdd + " spatial join ********************************")

      t0 = System.nanoTime()
      val leftRDD = leftrdd match {

        case "point" => new PointRDD(sc, points, FileDataSplitter.CSV, false, numPartitions, StorageLevel.MEMORY_ONLY)
        case "linestring" => new LineStringRDD(sc, linestrings, FileDataSplitter.WKT, false, numPartitions, StorageLevel.MEMORY_ONLY)
        case "rectangle" => new RectangleRDD(sc, rectangles, FileDataSplitter.WKT, false, numPartitions, StorageLevel.MEMORY_ONLY)
        case "polygon" => new PolygonRDD(sc, polygons, FileDataSplitter.WKT, false, numPartitions, StorageLevel.MEMORY_ONLY)

      }

      val rightRDD = rightrdd match {

        case "point" => new PointRDD(sc, points, FileDataSplitter.CSV, false, numPartitions, StorageLevel.MEMORY_ONLY)
        case "linestring" => new LineStringRDD(sc, linestrings, FileDataSplitter.WKT, false, numPartitions, StorageLevel.MEMORY_ONLY)
        case "rectangle" => new RectangleRDD(sc, rectangles, FileDataSplitter.WKT, false, numPartitions, StorageLevel.MEMORY_ONLY)
        case "polygon" => new PolygonRDD(sc, polygons, FileDataSplitter.WKT, false, numPartitions, StorageLevel.MEMORY_ONLY)

      }
      t1 = System.nanoTime()

      val read_time = (t1 - t0) / 1E9
      println("Read Time: " + read_time + " sec")

      leftRDD.spatialPartitioning(partitioningScheme)

      leftRDD.buildIndex(idx, true)

      leftRDD.indexedRDD.persist(StorageLevel.MEMORY_ONLY)

      leftRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)

      val c1 = leftRDD.spatialPartitionedRDD.count()

      val c2 = leftRDD.indexedRDD.count()

      leftRDD.rawSpatialRDD.unpersist()
      t1 = System.nanoTime()
      val leftPTime = (t1 - t0) / 1E9
      println("Left Partitioning and Indexing Time: " + leftPTime + " sec")

      t0 = System.nanoTime()

      rightRDD.spatialPartitioning(leftRDD.getPartitioner)

      rightRDD.spatialPartitionedRDD.persist(StorageLevel.MEMORY_ONLY)

      val c3 = rightRDD.spatialPartitionedRDD.count()

      t1 = System.nanoTime()
      val rightPTime = (t1 - t0) / 1E9
      println("Right Partitioning Time: " + rightPTime + " sec")

      rightRDD.rawSpatialRDD.unpersist()

      t0 = System.nanoTime()

      count = JoinQuery.SpatialJoinQuery(leftRDD, rightRDD, true, false).count()

      t1 = System.nanoTime()
      val join_time = (t1 - t0) / 1E9
      println("Join Time: " + join_time + " sec")

      val total_time = read_time + leftPTime + rightPTime + join_time

      println("Total Join Time: " + total_time + " sec")

      println("********************************************************************************************")

      leftRDD.spatialPartitionedRDD.unpersist()
      leftRDD.indexedRDD.unpersist()
      rightRDD.spatialPartitionedRDD.unpersist()
    }
  }
}
