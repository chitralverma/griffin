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

import java.util.Locale

import scala.collection.mutable.{HashMap => MutableMap}

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.StreamingQuery

import org.apache.griffin.measure.configuration.dqdefinition.{AppConfig, SinkParam}
import org.apache.griffin.measure.utils.{HdfsUtil, JsonUtil}
import org.apache.griffin.measure.utils.ParamUtil._

/**
 * sink metric and record to hdfs
 */
class FileSink(val appConfig: AppConfig, val sinkParam: SinkParam) extends Sink {

  private val OptionsStr: String = "options"
  private val FormatStr: String = "format"
  private val RecordsPathStr: String = "recordsPath"
  private val MetricsPathStr: String = "metricsPath"
  private val OutputModeStr: String = "outputMode"
  private val PartitionedByStr: String = "partitionedBy"

  private val Delimiter: String = "delimiter"
  private val Separator = "/"
  private val TabDelimiter: String = "\t"

  private val DefaultFormat: String = SQLConf.DEFAULT_DATA_SOURCE_NAME.defaultValueString
  private val partitionedBy: Seq[String] = config.getStringArr(PartitionedByStr, Nil)
  private val SupportedFormats: Seq[String] =
    Seq("parquet", "orc", "avro", "text", "csv", "tsv", "json")
  private var format: String = config.getString(FormatStr, DefaultFormat).toLowerCase(Locale.ROOT)
  var recordsPath: String = config.getString(RecordsPathStr, null)
  var metricsPath: String = config.getString(MetricsPathStr, null)
  val mode: SaveMode =
    SaveMode.valueOf(
      config.getString(OutputModeStr, "Append").toLowerCase(Locale.ROOT).capitalize)
  val options: MutableMap[String, String] = MutableMap(
    config.getParamStringMap(OptionsStr, Map.empty).toSeq: _*)

  assert(recordsPath != null, s"Mandatory config '$RecordsPathStr' is not defined.")
  assert(metricsPath != null, s"Mandatory config '$MetricsPathStr' is not defined.")
  assert(
    SupportedFormats.contains(format),
    s"Invalid format '$format' specified. Must be one of ${SupportedFormats.mkString("['", "', '", "']")}")

  if (format == "tsv") {
    format = "csv"
    options.getOrElseUpdate(Delimiter, TabDelimiter)
  }

  recordsPath = s"${if (recordsPath.endsWith(Separator)) recordsPath
  else s"$recordsPath$Separator"}/$jobName"
  metricsPath = s"${if (metricsPath.endsWith(Separator)) metricsPath
  else s"$metricsPath$Separator"}/$jobName/metrics.json"

  override def open(): Unit = {
    info(s"Opened FileBasedSink for job with name '$jobName', and applicationId '$applicationID'")
  }

  override def close(): Unit = {
    info(s"Closed FileBasedSink for job with name '$jobName'")
  }

  override def sinkBatchMetrics(metrics: Map[String, Any]): Unit = {
    val json = JsonUtil.toJson(metrics)
    HdfsUtil.withHdfsFile(metricsPath, appendIfExists = false) { out =>
      out.write((json + "\n").getBytes("utf-8"))
    }

  }

  override def sinkBatchRecords(measureName: String, dataset: DataFrame): Unit = {
    val dfWriter = dataset.write.format(format).options(options).mode(mode)

    {
      if (partitionedBy.isEmpty) dfWriter else dfWriter.partitionBy(partitionedBy: _*)
    }.save(s"$recordsPath/$measureName/")
  }

//  /**
//   * Implementation of persisting records for streaming pipelines.
//   */
//  override def sinkStreamingRecords(dataset: DataFrame): StreamingQuery = {
//    assert(dataset.isStreaming, "The given dataset is not steaming.")
//
//    dataset.writeStream
//      .format(format)
//      .outputMode(sinkParam.getStreamingOutputMode)
//      .trigger(sinkParam.getTrigger)
//      .options(options)
//      .queryName(sinkParam.getName)
//      .start(recordsPath)
//  }
}
