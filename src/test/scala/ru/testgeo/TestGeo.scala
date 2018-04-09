package ru.testgeo

import java.sql.Timestamp

import org.scalatest.{Matchers, WordSpecLike}
import ru.geo._

class TestGeo extends WordSpecLike with Matchers with SparkTest {

  import spark.implicits._

  "Extract name schema" in {
    val testRes = GeoAnalysis.extractUserName(spark.emptyDataset[Geodata])

    testRes._1.collect() shouldBe Array()

    testRes._2.collect() shouldBe Array()

  }

  "Extract name logic" in {

    val testData = Seq(
      Geodata.prototype.copy(user_id = Some("wifi_john_123")),
      Geodata.prototype.copy(user_id = Some("wifi_john_123")),
      Geodata.prototype.copy(user_id = Some("wifi_jon_123")),
      Geodata.prototype.copy(user_id = Some("wifi_jhon_123")),
      Geodata.prototype.copy(user_id = Some("wifi_bobbby_12")),
      Geodata.prototype.copy(user_id = Some("wifi_boby_12")),
      Geodata.prototype.copy(user_id = Some("wifi_booby_12"))
    ).toDS()

    val testRes = GeoAnalysis.extractUserName(testData)

    testRes._1.collect() shouldBe Seq(
      GeodataUserId.prototype.copy(user_id = Some("wifi_john_123"), user_num_id = Some("123")),
      GeodataUserId.prototype.copy(user_id = Some("wifi_john_123"), user_num_id = Some("123")),
      GeodataUserId.prototype.copy(user_id = Some("wifi_jon_123"), user_num_id = Some("123")),
      GeodataUserId.prototype.copy(user_id = Some("wifi_jhon_123"), user_num_id = Some("123")),
      GeodataUserId.prototype.copy(user_id = Some("wifi_bobbby_12"), user_num_id = Some("12")),
      GeodataUserId.prototype.copy(user_id = Some("wifi_boby_12"), user_num_id = Some("12")),
      GeodataUserId.prototype.copy(user_id = Some("wifi_booby_12"), user_num_id = Some("12"))
    ).toDS().collect()

    testRes._2.collect() shouldBe Seq(
      UserInfo.prototype.copy(user_num_id = Some("123"), user_name = Some("john")),
      UserInfo.prototype.copy(user_num_id = Some("12"), user_name = Some("bobbby"))
    ).toDS().collect()

  }

  "Check payment schema" in {
    GeoAnalysis.checkPayments(spark.emptyDataset[GeodataUserId]).collect() shouldBe Array()
  }

  "Check payment logic" in {

    val testData = Seq(
      GeodataUserId.prototype.copy(
        user_num_id = Some("1"), date = Some(Timestamp.valueOf("2017-01-01 00:00:00")), income_traf = Some(1.0),
        incomeUnitPrice = Some(2.0), outcome_traf = Some(1.0), outcomeUnitPrice = Some(1.0), payment = Some(3.0)
      )
      ,GeodataUserId.prototype.copy(
        user_num_id = Some("1"), date = Some(Timestamp.valueOf("2017-01-01 00:00:00")), income_traf = Some(2.0),
        incomeUnitPrice = Some(2.0), outcome_traf = Some(2.0), outcomeUnitPrice = Some(1.0), payment = Some(6.0)
      )
      ,GeodataUserId.prototype.copy(
        user_num_id = Some("1"), date = Some(Timestamp.valueOf("2017-01-02 00:00:00")), income_traf = Some(1.0),
        incomeUnitPrice = Some(2.0), outcome_traf = Some(1.0), outcomeUnitPrice = Some(1.0), payment = Some(2.0)
      )
      ,GeodataUserId.prototype.copy(
        user_num_id = Some("1"), date = Some(Timestamp.valueOf("2017-01-02 00:00:00")), income_traf = Some(2.0),
        incomeUnitPrice = Some(2.0), outcome_traf = Some(2.0), outcomeUnitPrice = Some(1.0), payment = Some(5.0)
      )
    ).toDS()

    GeoAnalysis.checkPayments(testData).collect() shouldBe
    Seq(
      PaymentInfo.prototype.copy(user_num_id = Some("1"), date = Some(Timestamp.valueOf("2017-01-01 00:00:00")), Some(true))
      , PaymentInfo.prototype.copy(user_num_id = Some("1"), date = Some(Timestamp.valueOf("2017-01-02 00:00:00")), Some(false))
    ).toDS().collect()

  }

  "Geo group schema" in {
    GeoAnalysis.defineGeoDeviceGroups(spark.emptyDataset[Geodata]).collect shouldBe Array()
  }

  "Geo group logic" in {

    val testData = Seq(
      Geodata.prototype.copy(device_id = Some("1_close_to_2_3"), lattitude = Some(52.50), longitude =  Some(65.60)),
      Geodata.prototype.copy(device_id = Some("2_close_to_1_3"), lattitude = Some(52.501), longitude =  Some(65.601)),
      Geodata.prototype.copy(device_id = Some("3_close_to_1_2"), lattitude = Some(52.502), longitude =  Some(65.602)),
      Geodata.prototype.copy(device_id = Some("4_close_to_5"), lattitude = Some(52.65), longitude =  Some(65.05)),
      Geodata.prototype.copy(device_id = Some("5_close_to_4"), lattitude = Some(52.651), longitude =  Some(65.051)),
      Geodata.prototype.copy(device_id = Some("6_close_to_nothing"), lattitude = Some(51.00), longitude =  Some(65.00))
    ).toDS()

    GeoAnalysis.defineGeoDeviceGroups(testData)
  }

  "Test GeoAnalysis main" in {
    GeoAnalysis.main(Array())
  }


}