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

package org.apache.griffin.measure.sink

import org.apache.spark.sql.DataFrame

import org.apache.griffin.measure.Loggable
import org.apache.griffin.measure.configuration.dqdefinition.{AppConfig, SinkParam}
import org.apache.griffin.measure.utils.SparkSessionFactory

/**
 * Base trait for metrics, batch and streaming Sinks.
 * To implement custom sinks, extend your classes with this trait.
 */
trait Sink extends Loggable with Serializable {
  val appConfig: AppConfig
  val sinkParam: SinkParam

  val jobName: String = appConfig.getName
  val config: Map[String, Any] = sinkParam.getConfig

  /**
   * Spark Application ID
   */
  val applicationID: String = SparkSessionFactory.getInstance.sparkContext.applicationId

  /**
   * Ensures that the pre-requisites (if any) of the Sink are met before opening it.
   */
  def validate(): Unit = {}

  /**
   * Allows initialization of the connection to the sink (if required).
   *
   */
  def open(): Unit = {}

  /**
   * Implementation of persisting metrics.
   */
  def sinkBatchMetrics(metrics: Map[String, Any]): Unit

  /**
   * Implementation of persisting records for batch pipelines.
   */
  def sinkBatchRecords(measureName: String, dataset: DataFrame): Unit

  /**
   * Allows clean up for the sink (if required).
   */
  def close(): Unit = {}

}
