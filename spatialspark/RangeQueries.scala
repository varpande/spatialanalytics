package spatialspark.measurements

import spatialspark.index.IndexConf
import spatialspark.index.STIndex
import spatialspark.util.MBR
import com.vividsolutions.jts.geom.{Envelope, GeometryFactory}
import com.vividsolutions.jts.io.WKTReader
import spatialspark.operator.SpatialOperator
import spatialspark.query.RangeQuery
import org.apache.spark.{SparkContext, SparkConf}

object RangeQueries {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("SpatialSpark Range Queries")
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

    point(points, "Points")
    linestring(linestrings, "Linestrings")
    query(polygons, "Polygons")
    query(rectangles, "Rectangles")

    sc.stop()

    def point(input: String, geomType: String) {

      val nQueries = 100
      var t0 = 0L
      var t1 = 0L
      var count1 = 0L

      val rangeQueryWindow1 = new GeometryFactory().toGeometry(new Envelope(-50.3010141441, -24.9526465797, -53.209588996, -30.1096863746))
      val rangeQueryWindow2 = new GeometryFactory().toGeometry(new Envelope(-54.4270741441, -24.9526465797, -53.209588996, -30.1096863746))
      val rangeQueryWindow3 = new GeometryFactory().toGeometry(new Envelope(-114.4270741441, 42.9526465797, -54.509588996, -27.0106863746))
      val rangeQueryWindow4 = new GeometryFactory().toGeometry(new Envelope(-82.7638020000, 42.9526465797, -54.509588996, 38.0106863746))
      val rangeQueryWindow5 = new GeometryFactory().toGeometry(new Envelope(-140.99778, 5.7305630159, -52.6480987209, 83.23324))
      val rangeQueryWindow6 = new GeometryFactory().toGeometry(new Envelope(-180.0, 180.0, -90.0, 90.0))

      val inputData = sc.textFile(input).map(x => (new WKTReader).read(x.split("\t").apply(0)))
      val inputDataWithId = inputData.zipWithIndex().map(_.swap).cache()
      val count = inputDataWithId.count()

      // Dry run
      t0 = System.nanoTime()
      for (i <- 1 to 20) {
        count1 = RangeQuery(sc, inputDataWithId, rangeQueryWindow1, SpatialOperator.Within, 0.0).count()
      }
      t1 = System.nanoTime()

      // Actual Measurements
      println("************************************ " + geomType + " range queries ************************************")

      println("Range1: ")
      t0 = System.nanoTime()
      for (i <- 1 to nQueries) {
        count1 = RangeQuery(sc, inputDataWithId, rangeQueryWindow1, SpatialOperator.Within, 0.0).count()
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
        count1 = RangeQuery(sc, inputDataWithId, rangeQueryWindow2, SpatialOperator.Within, 0.0).count()
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
        count1 = RangeQuery(sc, inputDataWithId, rangeQueryWindow3, SpatialOperator.Within, 0.0).count()
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
        count1 = RangeQuery(sc, inputDataWithId, rangeQueryWindow4, SpatialOperator.Within, 0.0).count()
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
        count1 = RangeQuery(sc, inputDataWithId, rangeQueryWindow5, SpatialOperator.Within, 0.0).count()
      }
      t1 = System.nanoTime()
      println("Count: " + count1)
      println("Selection Ratio: " + ((count1 * 100.0) / count))
      println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
      println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")

      println("Range6: ")
      t0 = System.nanoTime()
      for (i <- 1 to nQueries) {
        count1 = RangeQuery(sc, inputDataWithId, rangeQueryWindow6, SpatialOperator.Within, 0.0).count()
      }
      t1 = System.nanoTime()
      println("Count: " + count1)
      println("Selection Ratio: " + ((count1 * 100.0) / count))
      println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
      println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")
      t1 = 0L
      t0 = 0L

      inputDataWithId.unpersist()
    }

    def linestring(input: String, geomType: String) {

      val nQueries = 100
      var t0 = 0L
      var t1 = 0L
      var count1 = 0L

      val rangeQueryWindow1 = new GeometryFactory().toGeometry(new Envelope(-50.204, -24.9526465797, -53.209588996, -30.1096863746))
      val rangeQueryWindow2 = new GeometryFactory().toGeometry(new Envelope(-52.1270741441, -24.9526465797, -53.209588996, -30.1096863746))
      val rangeQueryWindow3 = new GeometryFactory().toGeometry(new Envelope(-94.4270741441, 22.9526465797, -34.609588996, -27.0106863746))
      val rangeQueryWindow4 = new GeometryFactory().toGeometry(new Envelope(-74.0938020000, 42.9526465797, -54.509588996, 38.0106863746))
      val rangeQueryWindow5 = new GeometryFactory().toGeometry(new Envelope(-150.99778, 7.2705630159, -52.6480987209, 83.23324))
      val rangeQueryWindow6 = new GeometryFactory().toGeometry(new Envelope(-180.0, 180.0, -90.0, 90.0))

      val inputData = sc.textFile(input).map(x => (new WKTReader).read(x.split("\t").apply(0)))
      val inputDataWithId = inputData.zipWithIndex().map(_.swap).cache()
      val count = inputDataWithId.count()
      t0 = System.nanoTime()

      // Dry run
      for (i <- 1 to 20) {
        count1 = RangeQuery(sc, inputDataWithId, rangeQueryWindow1, SpatialOperator.Within, 0.0).count()
      }
      t1 = System.nanoTime()

      // Actual Measurements
      println("************************************ " + geomType + " range queries ************************************")

      println("Range1: ")
      t0 = System.nanoTime()
      for (i <- 1 to nQueries) {
        count1 = RangeQuery(sc, inputDataWithId, rangeQueryWindow1, SpatialOperator.Within, 0.0).count()
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
        count1 = RangeQuery(sc, inputDataWithId, rangeQueryWindow2, SpatialOperator.Within, 0.0).count()
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
        count1 = RangeQuery(sc, inputDataWithId, rangeQueryWindow3, SpatialOperator.Within, 0.0).count()
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
        count1 = RangeQuery(sc, inputDataWithId, rangeQueryWindow4, SpatialOperator.Within, 0.0).count()
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
        count1 = RangeQuery(sc, inputDataWithId, rangeQueryWindow5, SpatialOperator.Within, 0.0).count()
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
        count1 = RangeQuery(sc, inputDataWithId, rangeQueryWindow6, SpatialOperator.Within, 0.0).count()
      }
      t1 = System.nanoTime()
      println("Count: " + count1)
      println("Selection Ratio: " + ((count1 * 100.0) / count))
      println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
      println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")

      inputDataWithId.unpersist()
    }

    def query(input: String, geomType: String) {

      val nQueries = 100
      var t0 = 0L
      var t1 = 0L
      var count1 = 0L

      val rangeQueryWindow1 = new GeometryFactory().toGeometry(new Envelope(-20.204, 17.9526465797, -53.209588996, -30.1096863746))
      val rangeQueryWindow2 = new GeometryFactory().toGeometry(new Envelope(-20.204, 20.4376465797, -53.209588996, -30.1096863746))
      val rangeQueryWindow3 = new GeometryFactory().toGeometry(new Envelope(-74.4270741441, 72.9526465797, -34.609588996, -6.5906863746))
      val rangeQueryWindow4 = new GeometryFactory().toGeometry(new Envelope(-104.0938020000, 118.9526465797, -54.509588996, 40.2406863746))
      val rangeQueryWindow5 = new GeometryFactory().toGeometry(new Envelope(-174.4270741441, 72.9526465797, -34.609588996, 48.4396863746))
      val rangeQueryWindow6 = new GeometryFactory().toGeometry(new Envelope(-180.0, 180.0, -90.0, 90.0))

      val inputData = sc.textFile(input).map(x => (new WKTReader).read(x.split("\t").apply(0)))
      val inputDataWithId = inputData.zipWithIndex().map(_.swap).cache()
      val count = inputDataWithId.count()
      t0 = System.nanoTime()

      // Dry run
      for (i <- 1 to 20) {
        count1 = RangeQuery(sc, inputDataWithId, rangeQueryWindow1, SpatialOperator.Within, 0.0).count()
      }
      t1 = System.nanoTime()

      // Actual Measurements
      println("************************************ " + geomType + " range queries ************************************")

      println("Range1: ")
      t0 = System.nanoTime()
      for (i <- 1 to nQueries) {
        count1 = RangeQuery(sc, inputDataWithId, rangeQueryWindow1, SpatialOperator.Within, 0.0).count()
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
        count1 = RangeQuery(sc, inputDataWithId, rangeQueryWindow2, SpatialOperator.Within, 0.0).count()
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
        count1 = RangeQuery(sc, inputDataWithId, rangeQueryWindow3, SpatialOperator.Within, 0.0).count()
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
        count1 = RangeQuery(sc, inputDataWithId, rangeQueryWindow4, SpatialOperator.Within, 0.0).count()
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
        count1 = RangeQuery(sc, inputDataWithId, rangeQueryWindow5, SpatialOperator.Within, 0.0).count()
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
        count1 = RangeQuery(sc, inputDataWithId, rangeQueryWindow6, SpatialOperator.Within, 0.0).count()
      }
      t1 = System.nanoTime()
      println("Count: " + count1)
      println("Selection Ratio: " + ((count1 * 100.0) / count))
      println("Total Time: " + ((t1 - t0) / 1E9) + " sec")
      println("Throughput: " + (nQueries * 60) / ((t1 - t0) / (1E9)) + " queries/min")

      inputDataWithId.unpersist()
    }
  }
}
