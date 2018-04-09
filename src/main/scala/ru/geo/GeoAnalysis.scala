package ru.geo

import java.sql.Timestamp

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.expressions.Window._
import org.apache.spark.sql.functions._

object GeoAnalysis {

  def main(arg: Array[String]): Unit = {

    implicit val spark = SparkSession.builder
      .master("local[*]")
      .appName("Example")
      .getOrCreate()

    import spark.implicits._

    val data = mockData.toDS()

    val dataWithName = extractUserName(data)

    dataWithName._1.show(false)
    dataWithName._2.show(false)

    val checkedPayments = checkPayments(dataWithName._1)

    checkedPayments.show(false)

    val devicesGeoGroups = defineGeoDeviceGroups(data)

    devicesGeoGroups.show(false)

  }

  /*Its always wise organise dataflow (and desired by author) as one pipe, that's why return type of this function is tuple,
  * but for the test sake we'll stop piping after using this one, just demonstrating common approach. So we are going to
  * produce side effects after this function and each of following.
  */
  def extractUserName(trafficData: Dataset[Geodata])(implicit spark: SparkSession): (Dataset[GeodataUserId], Dataset[UserInfo]) = {

    import spark.implicits._

    val extractStringByPos: (String, Int, String) => String = (delimeter, position, initialString) =>
      initialString.split(delimeter)(position)

    val extractStringByPosUdf = udf(extractStringByPos)

    (
      trafficData.withColumn("user_num_id", extractStringByPosUdf(lit("_"), lit(2), $"user_id")).as[GeodataUserId]
      , trafficData
      .withColumn("user_name_mist", extractStringByPosUdf(lit("_"), lit(1), $"user_id"))
      .withColumn("user_num_id", extractStringByPosUdf(lit("_"), lit(2), $"user_id"))
      .groupBy($"user_num_id", $"user_name_mist")
      .count
      .withColumn("_rn", row_number().over(partitionBy("user_num_id").orderBy($"count".desc)))
      .drop("_rn", "count")
      .withColumnRenamed("user_name_mist", "user_name")
      .where($"_rn" === 1).as[UserInfo]
    )

  }

  // assuming that data for analysis can be provided not only for one day, should add grouping by date
  def checkPayments(trafficData: Dataset[GeodataUserId])(implicit spark: SparkSession): Dataset[PaymentInfo] = {
    import spark.implicits._

    trafficData
      .groupBy($"user_num_id", $"date")
      .agg(
        sum(
          $"income_traf" * $"incomeUnitPrice" +
            $"outcome_traf" * $"outcomeUnitPrice"
        ) as "expectedPaymnet"
        , sum($"payment") as "factPayment"
      )
      .withColumn("isPaymentCorrect", $"expectedPaymnet" === $"factPayment")
      .drop("expectedPaymnet", "factPayment").as[PaymentInfo]
  }

  /*
  * According to the task grouping is based on the user id (which is uniquely backed by its own suffix) and geo coordinates,
  * implementating this logic, we will consider that both coordinates can posses enough precision to uniquely identify access
  * point, therefore grouping by coordinates or by deviceID is insensible. So it seems that this part of task is analogous to
  * forming new surrogate key is equivalent to the user_id exluding middle part, so taken from example wifi_75 (20.27, 30.37)
  * and zone_505 (20.25, 30.338) will be treated as different groups without any explicit mentioning of grouping by distance
  * criterium.
  *
  * Author of this code assumes that it was up to him to choose this distance, but such kind of grouping based on distance
  * looks at first glance strange as later there will be more precise group definition based on the last part of task
  *
  * In main we are going to omit this function call, it exists only to provide place to write this comments)))
   */

  def defineGroups(trafficData: Dataset[GeodataUserId])(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    val excludeMiddlePart: (String) => String = {
      (initialString) =>
        val parts = initialString.split('_')
        s"${parts(0)}_${parts(2)}"
    }

    val extractStringByPosUdf = udf(excludeMiddlePart)

    trafficData
      .withColumn("groupId", extractStringByPosUdf($"user_id"))
  }


  /*
  * Like in manifold theory, local geo group device to current will be defined as union of all manifolds that intersect current
  * while fullfilling certain restrictions. By single device manifold we understand a circle of certain radius, that
  * obviously can depend on goals we in pursuit of, therefore this radius for cities differs from countryside, as the
  * device concentration is lesser in countryside. For now there is only one restriction, centers of intersecting manifolds should
  * be situated no further then 1,5 of manifold radius.
  *
  * Note this operation is costly due to cartesian. It will be a good sense to use it only while working with prearranged
  * local device clusters.
  */
  def defineGeoDeviceGroups(trafficData: Dataset[Geodata])(implicit spark: SparkSession): Dataset[DeviceGeoGroup] = {
    import spark.implicits._

    val calcGeoDist: (Double, Double, Double, Double) => Double = {
      case (lat1, lon1, lat2, lon2) =>
        val R = 6371e3
        val phi1 = Math.toRadians(lat1)
        val phi2 = Math.toRadians(lat2)
        val deltaPhi = Math.toRadians(lat2 - lat1)
        val deltaL = Math.toRadians(lon2 - lon1)
        val a = Math.sin(deltaPhi / 2) * Math.sin(deltaPhi / 2) +
          Math.cos(phi1) * Math.cos(phi2) *
            Math.sin(deltaL / 2) * Math.sin(deltaL / 2)
        val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
        R * c
    }

    val calcGeoDistUdf = udf(calcGeoDist)

    val td1 = trafficData.as("1")

    val td2 = trafficData.as("2")

    td1.crossJoin(td2)
      .withColumn("distance", calcGeoDistUdf($"1.lattitude", $"1.longitude", $"2.lattitude", $"2.longitude"))
      .where($"1.device_id" =!= $"2.device_id")
      .groupBy($"1.device_id")
      .agg(
        collect_list(
          when(
            $"distance" < groupLocalRad
            , $"2.device_id"
          )
        ) as "localGroup"
      )
      .select($"1.device_id", $"localGroup").as[DeviceGeoGroup]
  }

}
