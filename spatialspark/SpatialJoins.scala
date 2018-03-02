package spatialspark.measurements

import spatialspark.index.IndexConf
import spatialspark.index.STIndex
import spatialspark.util.MBR
import com.vividsolutions.jts.geom.{Envelope, GeometryFactory}
import com.vividsolutions.jts.io.WKTReader
import spatialspark.operator.SpatialOperator
import spatialspark.query.RangeQuery
import spatialspark.operator.SpatialOperator
import spatialspark.partition.bsp.BinarySplitPartitionConf
import spatialspark.partition.fgp.FixedGridPartitionConf
import spatialspark.partition.stp.SortTilePartitionConf
import spatialspark.join.{BroadcastSpatialJoin, PartitionedSpatialJoin}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import scala.util.Try

object SpatialJoins {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("SpatialSpark Spatial Joins")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", "spatialspark.util.KyroRegistrator")
    val sc = new SparkContext(conf)

    val points = "/data/points_200M_wkt.csv"
    val polygons = "/data/buildings_114M.csv"
    val rectangles = "/data/rectangles_114M.csv"
    val linestrings = "/data/linestrings_72M.csv"

    def time[R](block: => R): R = {
      val t0 = System.nanoTime()
      val result = block // call-by-name
      val t1 = System.nanoTime()
      println("Query Runtime: " + (t1 - t0) / 1E9 + " sec ")
      result
    }

    query(points, points, "Points", "Points")
    query(points, linestrings, "Points", "LineStrings")
    query(points, rectangles, "Points", "Rectangles")
    query(points, polygons, "Points", "Polygons")
    query(linestrings, linestrings, "LineStrings", "LineStrings")
    query(linestrings, rectangles, "LineStrings", "Rectangles")
    query(linestrings, polygons, "LineStrings", "Polygons")
    query(rectangles, rectangles, "Rectangles", "Rectangles")
    query(rectangles, polygons, "Rectangles", "Polygons")
    query(polygons, polygons, "Polygons", "Polygons")

    sc.stop()

    def query(leftInput: String, rightInput: String, leftGeomType: String, rightGeomType: String) {

      var t0 = 0L
      var t1 = 0L
      var count1 = 0L
      var count = 0L
      val nQueries = 10
      val extentString = ""
      val method = "stp"

      val rangeQueryWindow6 = new GeometryFactory().toGeometry(new Envelope(-180.0, 180.0, -90.0, 90.0))

      println("************************************ " + leftGeomType + "-" + rightGeomType + " Spatial Join ************************************")

      t0 = System.nanoTime()
      val leftData = sc.textFile(leftInput, 1024).map(x => x.split("\t")).zipWithIndex()
      val leftGeometryById = leftData.map(x => (x._2, Try(new WKTReader().read(x._1.apply(0))))).filter(_._2.isSuccess).map(x => (x._1, x._2.get)).cache()
      count = leftGeometryById.count()
      t1 = System.nanoTime()
      println(leftGeomType + " Read Time: " + ((t1 - t0) / 1E9) + " sec")
      val leftTime = (t1 - t0)

      t0 = System.nanoTime()
      val rightData = sc.textFile(rightInput, 1024).map(x => x.split("\t")).zipWithIndex()
      val rightGeometryById = rightData.map(x => (x._2, Try(new WKTReader().read(x._1.apply(0))))).filter(_._2.isSuccess).map(x => (x._1, x._2.get)).cache()
      count = rightGeometryById.count()
      t1 = System.nanoTime()
      println(rightGeomType + " Read Time: " + ((t1 - t0) / 1E9) + " sec")
      val rightTime = (t1 - t0)
      val read_time = (leftTime + rightTime) / 1E9
      println("Total Reading Time: " + read_time + " sec")


      var matchedPairs: RDD[(Long, Long)] = sc.emptyRDD
      t0 = System.nanoTime()
      val extent = extentString match {
        case "" =>
          val temp = leftGeometryById.map(x => x._2.getEnvelopeInternal).map(x => (x.getMinX, x.getMinY, x.getMaxX, x.getMaxY)).reduce((a, b) => (a._1 min b._1, a._2 min b._2, a._3 max b._3, a._4 max b._4))
          val temp2 = rightGeometryById.map(x => x._2.getEnvelopeInternal).map(x => (x.getMinX, x.getMinY, x.getMaxX, x.getMaxY)).reduce((a, b) => (a._1 min b._1, a._2 min b._2, a._3 max b._3, a._4 max b._4))
          (temp._1 min temp2._1, temp._2 min temp2._2, temp._3 max temp2._3, temp._4 max temp2._4)
        case _ => (extentString.split(":").apply(0).toDouble, extentString.split(":").apply(1).toDouble,
          extentString.split(":").apply(2).toDouble, extentString.split(":").apply(3).toDouble)
      }

      val partConf = method match {
        case "stp" =>
          val dimX = 32
          val dimY = 32
          val ratio = 0.3
          new SortTilePartitionConf(dimX, dimY, new MBR(extent._1, extent._2, extent._3, extent._4), ratio, true)
        case "bsp" =>
          val level = 32
          val ratio = 0.1
          new BinarySplitPartitionConf(ratio, new MBR(extent._1, extent._2, extent._3, extent._4), level, true)
        case _ =>
          val dimX = 32
          val dimY = 32
          new FixedGridPartitionConf(dimX, dimY, new MBR(extent._1, extent._2, extent._3, extent._4))
      }
      t1 = System.nanoTime()
      val extentTime = ((t1 - t0) / 1E9)
      println("Extent Time: " + extentTime + " sec")

      // Embed timers inside PartitionedJoin in SpatialSpark to get partitioning and indexing time. Subtract the partitioning and indexing time from join time below to get purely join time
      t0 = System.nanoTime()
      matchedPairs = PartitionedSpatialJoin(sc, leftGeometryById, rightGeometryById, SpatialOperator.Intersects, 0.0, partConf)
      count = matchedPairs.count()
      t1 = System.nanoTime()
      val time0 = ((t1 - t0) / 1E9)

      val total_time = time0 + read_time + extentTime
      println("Join Time (including partitioning and indexing time): " + time0 + " sec")
      println("Total Join Time: " + total_time + " sec")

      leftGeometryById.unpersist()
      rightGeometryById.unpersist()
    }
  }
}
