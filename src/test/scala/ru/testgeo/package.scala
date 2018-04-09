package ru

import org.apache.spark.sql.SparkSession
import ru.geo.LocalSession

package object testgeo {

  trait SparkTest {
    protected implicit val spark: SparkSession = LocalSession.localSpark
  }

}
