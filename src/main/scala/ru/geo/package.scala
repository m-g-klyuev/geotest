package ru

import java.sql.Timestamp
import java.nio.file.Files
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

package object geo {

  val localityRad = 1000.0 // meters

  val proximityRatio = 1.5

  val groupLocalRad = localityRad * proximityRatio

  case class Geodata(
                      date: Option[Timestamp],
                      device_id: Option[String],
                      longitude: Option[BigDecimal],
                      lattitude: Option[BigDecimal],
                      spended_time: Option[BigDecimal],
                      user_id: Option[String],
                      income_traf: Option[BigDecimal],
                      outcome_traf: Option[BigDecimal],
                      payment: Option[BigDecimal],
                      incomeUnitPrice: Option[BigDecimal],
                      outcomeUnitPrice: Option[BigDecimal]
                    )

  case class GeodataUserId(
                            date: Option[Timestamp],
                            device_id: Option[String],
                            longitude: Option[BigDecimal],
                            lattitude: Option[BigDecimal],
                            spended_time: Option[BigDecimal],
                            user_id: Option[String],
                            user_num_id: Option[String],
                            income_traf: Option[BigDecimal],
                            outcome_traf: Option[BigDecimal],
                            payment: Option[BigDecimal],
                            incomeUnitPrice: Option[BigDecimal],
                            outcomeUnitPrice: Option[BigDecimal]
                          )

  case class UserInfo(
                       user_num_id: Option[String],
                       user_name: Option[String]
                     )

  case class DeviceGeoGroup(
                             device_id: Option[String],
                             localGroup: Option[Array[String]] // Array of local group device id's
                           )

  // Neither 0 or 1 is a boolean flag, using god blessed Boolean
  case class PaymentInfo(
                          user_num_id: Option[String]
                          , date: Option[Timestamp]
                          , isPaymentCorrect: Option[Boolean]
                        )

  object Geodata {
    def prototype: Geodata = Geodata(
      date = None,
      device_id = None,
      longitude = None,
      lattitude = None,
      spended_time = None,
      user_id = None,
      income_traf = None,
      outcome_traf = None,
      payment = None,
      incomeUnitPrice = None,
      outcomeUnitPrice = None
    )
  }

  object GeodataUserId {
    def prototype: GeodataUserId = GeodataUserId(
      date = None,
      device_id = None,
      longitude = None,
      lattitude = None,
      spended_time = None,
      user_id = None,
      user_num_id = None,
      income_traf = None,
      outcome_traf = None,
      payment = None,
      incomeUnitPrice = None,
      outcomeUnitPrice = None
    )
  }

  object UserInfo {
    def prototype: UserInfo = UserInfo(
      user_num_id = None,
      user_name = None
    )
  }

  object DeviceGeoGroup {
    def prototype: DeviceGeoGroup = DeviceGeoGroup(
      device_id = None,
      localGroup = Some(Array())
    )
  }

  object PaymentInfo {
    def prototype: PaymentInfo = PaymentInfo(
      user_num_id = None,
      date = None,
      isPaymentCorrect = None
    )
  }

  object LocalSession {
    lazy val warehouseDir = Files.createTempDirectory("spark-warehouse")

    lazy val localSpark = {
      Logger.getLogger("org").setLevel(Level.WARN)
      Logger.getLogger("akka").setLevel(Level.WARN)

      SparkSession
        .builder()
        .appName("test")
        .master("local")
        .config("spark.sql.shuffle.partitions", 4)
        .config("spark.sql.warehouse.dir", warehouseDir.toUri.toString)
        .getOrCreate()
    }
  }

  /*
* assuming there is no need to generate test data that covers all possible threads of execution
* as every single function has been tested seperately
* */

  val mockData = Seq(
    Geodata.prototype.copy(
      device_id = Some("1CloseTo23"), lattitude = Some(52.50), longitude = Some(65.60),
      date = Some(Timestamp.valueOf("2017-01-01 00:00:00")), user_id = Some("1CloseTo23_john_123"),
      income_traf = Some(1.0), incomeUnitPrice = Some(1.0), outcome_traf = Some(1.0), outcomeUnitPrice = Some(1.0),
      payment = Some(2.0)
    )
    , Geodata.prototype.copy(
      device_id = Some("2CloseTo13"), lattitude = Some(52.501), longitude = Some(65.601),
      date = Some(Timestamp.valueOf("2017-01-01 00:00:00")), user_id = Some("2CloseTo13_john_123"),
      income_traf = Some(1.0), incomeUnitPrice = Some(1.0), outcome_traf = Some(1.0), outcomeUnitPrice = Some(1.0),
      payment = Some(2.0)
    )
    , Geodata.prototype.copy(
      device_id = Some("3CloseTo12"), lattitude = Some(52.502), longitude = Some(65.602),
      date = Some(Timestamp.valueOf("2017-01-01 00:00:00")), user_id = Some("3CloseTo12_jhon_123"),
      income_traf = Some(1.0), incomeUnitPrice = Some(1.0), outcome_traf = Some(1.0), outcomeUnitPrice = Some(1.0),
      payment = Some(2.0)
    )
    , Geodata.prototype.copy(
      device_id = Some("4CloseTo5"), lattitude = Some(52.65), longitude = Some(65.05),
      date = Some(Timestamp.valueOf("2017-01-01 00:00:00")), user_id = Some("4CloseTo5_bob_12"),
      income_traf = Some(1.0), incomeUnitPrice = Some(1.0), outcome_traf = Some(1.0), outcomeUnitPrice = Some(1.0),
      payment = Some(2.0)
    )
    , Geodata.prototype.copy(
      device_id = Some("5CloseTo4"), lattitude = Some(52.651), longitude = Some(65.051),
      date = Some(Timestamp.valueOf("2017-01-01 00:00:00")), user_id = Some("5CloseTo4_bob_12"),
      income_traf = Some(1.0), incomeUnitPrice = Some(1.0), outcome_traf = Some(1.0), outcomeUnitPrice = Some(1.0),
      payment = Some(2.0)
    )
    , Geodata.prototype.copy(
      device_id = Some("6CloseToNothing"), lattitude = Some(51.00), longitude = Some(65.00),
      date = Some(Timestamp.valueOf("2017-01-01 00:00:00")), user_id = Some("6CloseToNothing_bbob_12"),
      income_traf = Some(1.0), incomeUnitPrice = Some(1.0), outcome_traf = Some(1.0), outcomeUnitPrice = Some(1.0),
      payment = Some(1.0)
    )
  )

}
