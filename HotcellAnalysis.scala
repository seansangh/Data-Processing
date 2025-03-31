package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  // YOU NEED TO CHANGE THIS PART
  // Filter out the points that will not be considered for the analysis
  pickupInfo.createOrReplaceTempView("pickupInfo")
  val filteredPoints = spark.sql(
    "SELECT x, y, z, COUNT(*) AS pointCounts " +
      "FROM pickupInfo " +
      "WHERE x>=" + minX + " AND x<=" + maxX + " and y>="+minY +" and y<="+maxY+" and z>="+minZ+" and z<=" +maxZ + " " +
      "GROUP BY x,y,z"
  ).persist()
  filteredPoints.createOrReplaceTempView("filteredPoints")

  // Calculating the sums required for the ZScore calculations
  val sums = spark.sql(
    "SELECT SUM(pointCounts) AS totalPoints, SUM(pointCounts*pointCounts) AS sumOfSquaredPointCounts " +
      "FROM filteredPoints").persist()
  val totalPoints = sums.first().getLong(0).toDouble
  val sumOfSquaredPointCounts = sums.first().getLong(1).toDouble

  // Calculate the mean and standard deviation once required for further calculations
  val meanX = totalPoints / numCells
  val stdDev = Math.sqrt((sumOfSquaredPointCounts/numCells) - (meanX*meanX))

  val aggregatedNeighbors = spark.sql(
    "SELECT t1.x AS x, t1.y AS y, t1.z AS z, COUNT(*) AS neighbors, SUM(t2.pointCounts) AS sumOfNeighborCounts " +
    "FROM filteredPoints AS t1 " +
    "INNER JOIN filteredPoints AS t2 " +
    "ON ((abs(t1.x-t2.x) <= 1 AND abs(t1.y-t2.y) <= 1 AND abs(t1.z-t2.z) <= 1)) " +
    "GROUP BY t1.x, t1.y, t1.z"
  ).persist()
  aggregatedNeighbors.createOrReplaceTempView("aggregatedNeighbors")

  spark.udf.register("ZScore", (mean: Double, stddev: Double, neighbors: Int, sumOfNeighborCounts: Int, numCells:Int)=>
    HotcellUtils.ZScore(mean, stddev, neighbors, sumOfNeighborCounts, numCells)
  )

  // Calculate the ZScores of all
  val dfWithZScore =  spark.sql(
    "SELECT x,y,z,ZScore("+ meanX + ","+ stdDev +",neighbors,sumOfNeighborCounts," + numCells+") " +
      "AS zscore FROM aggregatedNeighbors")
  dfWithZScore.createOrReplaceTempView("dfWithZScore")

  val retVal = spark.sql(
    "SELECT x, y, z " +
      "FROM dfWithZScore " +
      "ORDER BY zscore DESC"
  )
  retVal
 //  YOU NEED TO CHANGE THIS PART
}
}
