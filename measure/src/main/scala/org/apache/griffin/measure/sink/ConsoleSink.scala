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

import scala.collection.mutable.{HashMap => MutableMap}
import scala.util.Try

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQuery

import org.apache.griffin.measure.configuration.dqdefinition.{AppConfig, SinkParam}
import org.apache.griffin.measure.utils.JsonUtil
import org.apache.griffin.measure.utils.ParamUtil._

/**
 * Console Sink for Records and Metrics.
 * Records are shown in a tabular structure and Metrics are logged as JSON string.
 *
 * Supported Configurations:
 *  - truncate : [[Boolean]] Whether truncate long strings. If true, strings more than 20 characters
 *  will be truncated and all cells will be aligned right. Default is true.
 *  - numRows : [[Int]] Number of rows to show. Default is 20.
 */
class ConsoleSink(val appConfig: AppConfig, val sinkParam: SinkParam) extends Sink {

  private val OptionsStr: String = "options"
  private val Truncate: String = "truncate"
  private val NumberOfRows: String = "numRows"

  private val numRows: Int = config.getInt(NumberOfRows, 20)

  val options: MutableMap[String, String] = MutableMap(
    config.getParamStringMap(OptionsStr, Map.empty).toSeq: _*)
  private val truncateRecords: Boolean =
    Try(options.getOrElse(Truncate, "true").toBoolean).getOrElse(false)

  override def open(): Unit = {
    info(s"Opened ConsoleSink for job with name '$jobName' and applicationId '$applicationID'")
  }

  override def close(): Unit = {
    info(s"Closed ConsoleSink for job with name '$jobName'")
  }

  /**
   * Implementation of persisting metrics.
   */
  override def sinkBatchMetrics(metrics: Map[String, Any]): Unit = {
    info(s"Metrics for job with name '${appConfig.getName}':\n${JsonUtil.toJson(metrics)}")
  }

  override def sinkBatchRecords(measureName: String, dataset: DataFrame): Unit = {
    info(s"Records for measure with name '$measureName'")
    dataset.show(numRows, truncateRecords)
  }

//  /**
//   * Implementation of persisting records for streaming pipelines.
//   */
//  override def sinkStreamingRecords(dataset: DataFrame): StreamingQuery = {
//    assert(dataset.isStreaming, "The given dataset is not steaming.")
//
//    dataset.writeStream
//      .format("console")
//      .outputMode(sinkParam.getStreamingOutputMode)
//      .trigger(sinkParam.getTrigger)
//      .options(options)
//      .queryName(sinkParam.getName)
//      .start()
//  }
}
