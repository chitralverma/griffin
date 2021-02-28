package org.apache.griffin.measure.utils

import scala.util.{Failure, Success, Try}

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import org.apache.griffin.measure.Loggable

object SparkSessionFactory extends Loggable {

  private var sparkSession: SparkSession = _

  def create(sparkConfMap: Map[String, String]): SparkSession = {
    if (sparkSession == null)
      sparkSession = {
        info("Attempting to create an instance of SparkSession.")
        val sparkConf = new SparkConf().setAll(sparkConfMap)

        SparkSession
          .builder()
          .config(sparkConf)
          .enableHiveSupport()
          .getOrCreate()
      }

    sparkSession
  }

  def getInstance: SparkSession = {
    val instance = Option(sparkSession)
    assert(
      instance.isDefined,
      "Fatal Error: Unable to get instance of SparkSession as it has not been initialised yet.")

    instance.get
  }

  def close(): Unit = {
    Thread.sleep(5000)

    if (sparkSession != null) {
      Try(sparkSession.close()) match {
        case Success(_) =>
          info("Closed SparkSession Successfully")
        case Failure(ex) =>
          warn(s"Encountered '${ex.getLocalizedMessage}' while closing SparkSession", ex)
      }

      sparkSession = null
    } else warn("Tried closing an already closed SparkSession")
  }

}
