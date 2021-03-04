/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.griffin.measure.utils

import scala.util.{Failure, Success, Try}

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import org.apache.griffin.measure.Loggable

object SparkSessionFactory extends Loggable {

  private var sparkSession: SparkSession = _

  def create(sparkConfMap: Map[String, String]): SparkSession = {
    if (sparkSession == null) {
      sparkSession = {
        info("Attempting to create an instance of SparkSession.")
        val sparkConf = new SparkConf().setAll(sparkConfMap)

        SparkSession
          .builder()
          .config(sparkConf)
          .enableHiveSupport()
          .getOrCreate()
      }
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

    if (sparkSession != null) {
      Try(sparkSession.close()) match {
        case Success(_) =>
          info("Closed SparkSession Successfully")
        case Failure(ex) =>
          warn(s"Encountered '${ex.getLocalizedMessage}' while closing SparkSession", ex)
      }

      sparkSession = null
      Thread.sleep(5000)
    } else warn("Tried closing an already closed SparkSession")
  }

}
