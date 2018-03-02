package magellan.measurements

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.SizeEstimator
import org.apache.spark.rdd.RDD
import magellan._
import org.apache.spark.sql.magellan.dsl.expressions._
import org.apache.hadoop.mapreduce.TaskInputOutputContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.util.UUID
import org.apache.hadoop.mapreduce.TaskInputOutputContext
import magellan.index._
import fastparse.all._
import fastparse.core.Parsed.{Failure, Success}
import scala.collection.mutable.ListBuffer
import magellan.{BoundingBox, Point, Polygon, PolygonDeserializer}

object SpatialJoins {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Magellan Spatial Joins")
    val spark = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(spark)
    val sparkSession = SparkSession.builder().appName("Magellan Spatial Joins").getOrCreate()
    import sqlContext.implicits._

    def time[R](block: => R): R = {
      val t0 = System.nanoTime()
      val result = block // call-by-name
      val t1 = System.nanoTime()
      println("Join time: " + (t1 - t0) / 1E9 + " sec ")
      result
    }

    pointPoint()
    pointLineString()
    pointRectangle()
    pointPolygon()
    linestringLineString()
    linestringRectangle()
    linestringPolygon()
    rectangleRectangle()
    polygonRectangle()
    polygonPolygon()

    spark.stop()

    def pointLineString() {

      println("*************************** Point-LineString ****************************************")

      val precision = 30
      var count = 0L
      var t0 = 0L
      var t1 = 0L

      val beginTime = System.currentTimeMillis()
      magellan.Utils.injectRules(sparkSession)

      val rawPoints = spark.textFile("/data/points_200M.csv").map { line =>
        val parts = line.split(",")
        val longitude = parts(0).toDouble
        val latitude = parts(1).toDouble
        (UUID.randomUUID().toString(), Point(longitude, latitude))
      }.repartition(1024).toDF("id", "point")
      val roads = rawPoints.withColumn("index", $"point" index precision).select($"point", $"index")
      val indexedRoads = roads.withColumn("index", explode($"index")).select("point", "index.curve", "index.relation").cache()
      val count1 = indexedRoads.count()

      val readBuildings = spark.textFile("/data/linestrings_72M.csv", 1024)
      val rawBuildings = readBuildings.toDF("text").withColumn("polyline", wkt($"text")("polyline"))
      val buildings = rawBuildings.withColumn("index", $"polyline" index precision).select($"polyline", $"index")
      val indexedBuildings = buildings.withColumn("index", explode($"index")).select("polyline", "index.curve", "index.relation").cache()
      val count2 = indexedBuildings.count()

      val runtime = System.currentTimeMillis() - beginTime

      println("Indexing Time: " + (runtime) / 1E3 + " sec ")

      t0 = System.nanoTime()
      count = indexedBuildings.join(indexedRoads, indexedRoads("curve") === indexedBuildings("curve")).where((indexedBuildings("relation") === "Intersects")).count()
      t1 = System.nanoTime()
      val time0 = ((t1 - t0) / 1E9)
      println("Join Time: " + time0 + " sec")

      val total_time = (runtime / 1E3) + time0

      println("Total Join Time: " + total_time + " sec")
      println("")

      indexedRoads.unpersist()
      indexedBuildings.unpersist()
    }

    def pointPoint() {

      println("*************************** Point-Point ****************************************")

      val precision = 30
      var count = 0L
      var t0 = 0L
      var t1 = 0L

      val beginTime = System.currentTimeMillis()
      magellan.Utils.injectRules(sparkSession)

      val rawPoints = spark.textFile("/data/points_200M.csv").map { line =>
        val parts = line.split(",")
        val longitude = parts(0).toDouble
        val latitude = parts(1).toDouble
        (UUID.randomUUID().toString(), Point(longitude, latitude))
      }.repartition(1024).toDF("id", "point")
      val roads = rawPoints.withColumn("index", $"point" index precision).select($"point", $"index")
      val indexedRoads = roads.withColumn("index", explode($"index")).select("point", "index.curve", "index.relation").cache()
      val count1 = indexedRoads.count()

      val rawBuildings = spark.textFile("/data/points_200M.csv").map { line =>
        val parts = line.split(",")
        val longitude = parts(0).toDouble
        val latitude = parts(1).toDouble
        (UUID.randomUUID().toString(), Point(longitude, latitude))
      }.repartition(1024).toDF("id", "point")
      val buildings = rawBuildings.withColumn("index", $"point" index precision).select($"point", $"index")
      val indexedBuildings = buildings.withColumn("index", explode($"index")).select("point", "index.curve", "index.relation").cache()
      val count2 = indexedBuildings.count()

      val runtime = System.currentTimeMillis() - beginTime

      println("Indexing Time: " + (runtime) / 1E3 + " sec ")

      t0 = System.nanoTime()
      count = indexedBuildings.join(indexedRoads, indexedRoads("curve") === indexedBuildings("curve")).where((indexedBuildings("relation") === "Intersects")).count()
      t1 = System.nanoTime()
      val time0 = ((t1 - t0) / 1E9)
      println("Join Time: " + time0 + " sec")

      val total_time = (runtime / 1E3) + time0

      println("Total Join Time: " + total_time + " sec")
      println("")

      indexedRoads.unpersist()
      indexedBuildings.unpersist()
    }

    def pointPolygon() {

      println("*************************** Point-Polygon ****************************************")

      val precision = 30
      var count = 0L
      var t0 = 0L
      var t1 = 0L

      val beginTime = System.currentTimeMillis()
      magellan.Utils.injectRules(sparkSession)

      val rawPoints = spark.textFile("/data/points_200M.csv").map { line =>
        val parts = line.split(",")
        val longitude = parts(0).toDouble
        val latitude = parts(1).toDouble
        (UUID.randomUUID().toString(), Point(longitude, latitude))
      }.repartition(1024).toDF("id", "point")
      val roads = rawPoints.withColumn("index", $"point" index precision).select($"point", $"index")
      val indexedRoads = roads.withColumn("index", explode($"index")).select("point", "index.curve", "index.relation").cache()
      val count1 = indexedRoads.count()


      val readBuildings = spark.textFile("/data/buildings_114M.csv", 1024)
      val rawBuildings = readBuildings.toDF("text").withColumn("polygon", wkt($"text")("polygon"))
      val buildings = rawBuildings.withColumn("index", $"polygon" index precision).select($"polygon", $"index")
      val indexedBuildings = buildings.withColumn("index", explode($"index")).select("polygon", "index.curve", "index.relation").cache()
      val count2 = indexedBuildings.count()

      val runtime = System.currentTimeMillis() - beginTime

      println("Indexing Time: " + (runtime) / 1E3 + " sec ")

      t0 = System.nanoTime()
      count = indexedBuildings.join(indexedRoads, indexedRoads("curve") === indexedBuildings("curve")).where((indexedBuildings("relation") === "Intersects")).count()
      t1 = System.nanoTime()
      val time0 = ((t1 - t0) / 1E9)
      println("Join Time: " + time0 + " sec")

      val total_time = (runtime / 1E3) + time0

      println("Total Join Time: " + total_time + " sec")

      println("")

      indexedRoads.unpersist()
      indexedBuildings.unpersist()
    }

    def pointRectangle() {

      println("*************************** Point-Rectangle ****************************************")

      val precision = 30
      var count = 0L
      var t0 = 0L
      var t1 = 0L

      val beginTime = System.currentTimeMillis()
      magellan.Utils.injectRules(sparkSession)

      val rawPoints = spark.textFile("/data/points_200M.csv").map { line =>
        val parts = line.split(",")
        val longitude = parts(0).toDouble
        val latitude = parts(1).toDouble
        (UUID.randomUUID().toString(), Point(longitude, latitude))
      }.repartition(1024).toDF("id", "point")
      val roads = rawPoints.withColumn("index", $"point" index precision).select($"point", $"index")
      val indexedRoads = roads.withColumn("index", explode($"index")).select("point", "index.curve", "index.relation").cache()
      val count1 = indexedRoads.count()

      val readBuildings = spark.textFile("/data/rectangles_114M.csv", 1024)
      val rawBuildings = readBuildings.toDF("text").withColumn("polygon", wkt($"text")("polygon"))
      val buildings = rawBuildings.withColumn("index", $"polygon" index precision).select($"polygon", $"index")
      val indexedBuildings = buildings.withColumn("index", explode($"index")).select("polygon", "index.curve", "index.relation").cache()
      val count2 = indexedBuildings.count()

      val runtime = System.currentTimeMillis() - beginTime

      println("Indexing Time: " + (runtime) / 1E3 + " sec ")

      t0 = System.nanoTime()
      count = indexedBuildings.join(indexedRoads, indexedRoads("curve") === indexedBuildings("curve")).where((indexedBuildings("relation") === "Intersects")).count()
      t1 = System.nanoTime()
      val time0 = ((t1 - t0) / 1E9)
      println("Join Time: " + time0 + " sec")

      val total_time = (runtime / 1E3) + time0

      println("Total Join Time: " + total_time + " sec")

      println("")

      indexedRoads.unpersist()
      indexedBuildings.unpersist()
    }

    def linestringLineString() {

      println("*************************** LineString-LineString ****************************************")

      val precision = 30
      var count = 0L
      var t0 = 0L
      var t1 = 0L

      val beginTime = System.currentTimeMillis()
      magellan.Utils.injectRules(sparkSession)

      val readRoads = spark.textFile("/data/linestrings_72M.csv", 1024)
      val rawRoads = readRoads.toDF("text").withColumn("polyline", wkt($"text")("polyline"))
      val roads = rawRoads.withColumn("index", $"polyline" index precision).select($"polyline", $"index")
      val indexedRoads = roads.withColumn("index", explode($"index")).select("polyline", "index.curve", "index.relation").cache()
      val count1 = indexedRoads.count()

      val readBuildings = spark.textFile("/data/linestrings_72M.csv", 1024)
      val rawBuildings = readBuildings.toDF("text").withColumn("polyline", wkt($"text")("polyline"))
      val buildings = rawBuildings.withColumn("index", $"polyline" index precision).select($"polyline", $"index")
      val indexedBuildings = buildings.withColumn("index", explode($"index")).select("polyline", "index.curve", "index.relation").cache()
      val count2 = indexedBuildings.count()

      val runtime = System.currentTimeMillis() - beginTime

      println("Indexing Time: " + (runtime) / 1E3 + " sec ")

      t0 = System.nanoTime()
      count = indexedBuildings.join(indexedRoads, indexedRoads("curve") === indexedBuildings("curve")).where((indexedBuildings("relation") === "Intersects")).count()
      t1 = System.nanoTime()
      val time0 = ((t1 - t0) / 1E9)
      println("Join Time: " + time0 + " sec")

      val total_time = (runtime / 1E3) + time0

      println("Total Join Time: " + total_time + " sec")

      println("")

      indexedRoads.unpersist()
      indexedBuildings.unpersist()
    }

    def linestringPolygon() {

      println("*************************** LineString-Polygon ****************************************")

      val precision = 30
      var count = 0L
      var t0 = 0L
      var t1 = 0L

      val beginTime = System.currentTimeMillis()
      magellan.Utils.injectRules(sparkSession)

      val readRoads = spark.textFile("/data/linestrings_72M.csv", 1024)
      val rawRoads = readRoads.toDF("text").withColumn("polyline", wkt($"text")("polyline"))
      val roads = rawRoads.withColumn("index", $"polyline" index precision).select($"polyline", $"index")
      val indexedRoads = roads.withColumn("index", explode($"index")).select("polyline", "index.curve", "index.relation").cache()
      val count1 = indexedRoads.count()

      val readBuildings = spark.textFile("/data/buildings_114M.csv", 1024)
      val rawBuildings = readBuildings.toDF("text").withColumn("polygon", wkt($"text")("polygon"))
      val buildings = rawBuildings.withColumn("index", $"polygon" index precision).select($"polygon", $"index")
      val indexedBuildings = buildings.withColumn("index", explode($"index")).select("polygon", "index.curve", "index.relation").cache()
      val count2 = indexedBuildings.count()

      val runtime = System.currentTimeMillis() - beginTime

      println("Indexing Time: " + (runtime) / 1E3 + " sec ")

      t0 = System.nanoTime()
      count = indexedBuildings.join(indexedRoads, indexedRoads("curve") === indexedBuildings("curve")).where((indexedBuildings("relation") === "Intersects")).count()
      t1 = System.nanoTime()
      val time0 = ((t1 - t0) / 1E9)
      println("Join Time: " + time0 + " sec")

      val total_time = (runtime / 1E3) + time0

      println("Total Join Time: " + total_time + " sec")

      println("")

      indexedRoads.unpersist()
      indexedBuildings.unpersist()
    }

    def linestringRectangle() {

      println("*************************** LineString-Rectangle ****************************************")

      val precision = 30
      var count = 0L
      var t0 = 0L
      var t1 = 0L

      val beginTime = System.currentTimeMillis()
      magellan.Utils.injectRules(sparkSession)

      val readRoads = spark.textFile("/data/linestrings_72M.csv", 1024)
      val rawRoads = readRoads.toDF("text").withColumn("polyline", wkt($"text")("polyline"))
      val roads = rawRoads.withColumn("index", $"polyline" index precision).select($"polyline", $"index")
      val indexedRoads = roads.withColumn("index", explode($"index")).select("polyline", "index.curve", "index.relation").cache()
      val count1 = indexedRoads.count()

      val readBuildings = spark.textFile("/data/rectangles_114M.csv", 1024)
      val rawBuildings = readBuildings.toDF("text").withColumn("polygon", wkt($"text")("polygon"))
      val buildings = rawBuildings.withColumn("index", $"polygon" index precision).select($"polygon", $"index")
      val indexedBuildings = buildings.withColumn("index", explode($"index")).select("polygon", "index.curve", "index.relation").cache()
      val count2 = indexedBuildings.count()

      val runtime = System.currentTimeMillis() - beginTime

      println("Indexing Time: " + (runtime) / 1E3 + " sec ")

      t0 = System.nanoTime()
      count = indexedBuildings.join(indexedRoads, indexedRoads("curve") === indexedBuildings("curve")).where((indexedBuildings("relation") === "Intersects")).count()
      t1 = System.nanoTime()
      val time0 = ((t1 - t0) / 1E9)
      println("Join Time: " + time0 + " sec")

      val total_time = (runtime / 1E3) + time0

      println("Total Join Time: " + total_time + " sec")

      println("")

      indexedRoads.unpersist()
      indexedBuildings.unpersist()
    }

    def polygonPolygon() {

      println("*************************** Polygon-Polygon ****************************************")

      val precision = 30
      var count = 0L
      var t0 = 0L
      var t1 = 0L

      val beginTime = System.currentTimeMillis()
      magellan.Utils.injectRules(sparkSession)

      val readRoads = spark.textFile("/data/buildings_114M.csv", 1024)
      val rawRoads = readRoads.toDF("text").withColumn("polygon", wkt($"text")("polygon"))
      val roads = rawRoads.withColumn("index", $"polygon" index precision).select($"polygon", $"index")
      val indexedRoads = roads.withColumn("index", explode($"index")).select("polygon", "index.curve", "index.relation").cache()
      val count1 = indexedRoads.count()

      val readBuildings = spark.textFile("/data/buildings_114M.csv", 1024)
      val rawBuildings = readBuildings.toDF("text").withColumn("polygon", wkt($"text")("polygon"))
      val buildings = rawBuildings.withColumn("index", $"polygon" index precision).select($"polygon", $"index")
      val indexedBuildings = buildings.withColumn("index", explode($"index")).select("polygon", "index.curve", "index.relation").cache()
      val count2 = indexedBuildings.count()

      val runtime = System.currentTimeMillis() - beginTime

      println("Indexing Time: " + (runtime) / 1E3 + " sec ")

      t0 = System.nanoTime()
      count = indexedBuildings.join(indexedRoads, indexedRoads("curve") === indexedBuildings("curve")).where((indexedBuildings("relation") === "Intersects")).count()
      t1 = System.nanoTime()
      val time0 = ((t1 - t0) / 1E9)
      println("Join Time: " + time0 + " sec")

      val total_time = (runtime / 1E3) + time0

      println("Total Join Time: " + total_time + " sec")

      println("")

      indexedRoads.unpersist()
      indexedBuildings.unpersist()
    }

    def polygonRectangle() {

      println("*************************** Rectangle-Polygon ****************************************")

      val precision = 30
      var count = 0L
      var t0 = 0L
      var t1 = 0L

      val beginTime = System.currentTimeMillis()
      magellan.Utils.injectRules(sparkSession)

      val readRoads = spark.textFile("/data/rectangles_114M.csv", 1024)
      val rawRoads = readRoads.toDF("text").withColumn("polygon", wkt($"text")("polygon"))
      val roads = rawRoads.withColumn("index", $"polygon" index precision).select($"polygon", $"index")
      val indexedRoads = roads.withColumn("index", explode($"index")).select("polygon", "index.curve", "index.relation").cache()
      val count1 = indexedRoads.count()

      val readBuildings = spark.textFile("/data/buildings_114M.csv", 1024)
      val rawBuildings = readBuildings.toDF("text").withColumn("polygon", wkt($"text")("polygon"))
      val buildings = rawBuildings.withColumn("index", $"polygon" index precision).select($"polygon", $"index")
      val indexedBuildings = buildings.withColumn("index", explode($"index")).select("polygon", "index.curve", "index.relation").cache()
      val count2 = indexedBuildings.count()

      val runtime = System.currentTimeMillis() - beginTime

      println("Indexing Time: " + (runtime) / 1E3 + " sec ")

      t0 = System.nanoTime()
      count = indexedBuildings.join(indexedRoads, indexedRoads("curve") === indexedBuildings("curve")).where((indexedBuildings("relation") === "Intersects")).count()
      t1 = System.nanoTime()
      val time0 = ((t1 - t0) / 1E9)
      println("Join Time: " + time0 + " sec")

      val total_time = (runtime / 1E3) + time0

      println("Total Join Time: " + total_time + " sec")

      println("")

      indexedRoads.unpersist()
      indexedBuildings.unpersist()
    }

    def rectangleRectangle() {

      println("*************************** Rectangle-Rectangle ****************************************")

      val precision = 30
      var count = 0L
      var t0 = 0L
      var t1 = 0L

      val beginTime = System.currentTimeMillis()
      magellan.Utils.injectRules(sparkSession)

      val readRoads = spark.textFile("/data/rectangles_114M.csv", 1024)
      val rawRoads = readRoads.toDF("text").withColumn("polygon", wkt($"text")("polygon"))
      val roads = rawRoads.withColumn("index", $"polygon" index precision).select($"polygon", $"index")
      val indexedRoads = roads.withColumn("index", explode($"index")).select("polygon", "index.curve", "index.relation").cache()
      val count1 = indexedRoads.count()

      val readBuildings = spark.textFile("/data/rectangles_114M.csv", 1024)
      val rawBuildings = readBuildings.toDF("text").withColumn("polygon", wkt($"text")("polygon"))
      val buildings = rawBuildings.withColumn("index", $"polygon" index precision).select($"polygon", $"index")
      val indexedBuildings = buildings.withColumn("index", explode($"index")).select("polygon", "index.curve", "index.relation").cache()
      val count2 = indexedBuildings.count()

      val runtime = System.currentTimeMillis() - beginTime

      println("Indexing Time: " + (runtime) / 1E3 + " sec ")

      t0 = System.nanoTime()
      count = indexedBuildings.join(indexedRoads, indexedRoads("curve") === indexedBuildings("curve")).where((indexedBuildings("relation") === "Intersects")).count()
      t1 = System.nanoTime()
      val time0 = ((t1 - t0) / 1E9)

      println("Join Time: " + time0 + " sec")

      val total_time = (runtime / 1E3) + time0

      println("Total Join Time: " + total_time + " sec")

      println("")

      indexedRoads.unpersist()
      indexedBuildings.unpersist()
    }
  }
}
