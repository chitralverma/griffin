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

package org.apache.griffin.measure.step.write

import scala.collection.immutable.{Map => HashMap}
import scala.util.Try

import org.apache.spark.sql.DataFrame

import org.apache.griffin.measure.configuration.enums.FlattenType._
import org.apache.griffin.measure.context.DQContext
import org.apache.griffin.measure.execution.StreamingQueryRegister
import org.apache.griffin.measure.sink.Sink
import org.apache.griffin.measure.utils.SparkSessionFactory

/**
 * write metrics into context metric wrapper
 */
case class MetricWriteStep(
    name: String,
    inputName: String,
    flattenType: FlattenType,
    writeTimestampOpt: Option[Long] = None)
    extends WriteStep {

  private val _MetricName = "metricName"
  private val _ApplicationID = "applicationID"
  private val _BatchID = "batchID"
  private val _MetricValue = "metricValue"

  val emptyMetricMap: HashMap[Long, HashMap[String, Any]] = HashMap[Long, HashMap[String, Any]]()
  val emptyMap: HashMap[String, Any] = HashMap.empty[String, Any]

  def execute(context: DQContext): Try[Boolean] = Try {
    flushMetrics(context)
    true
  }

  private def processBatch(df: DataFrame, batchId: Long): Map[String, Any] = {
    val rows = df.collect()
    val columns = df.columns
    val metricMaps: Seq[Map[String, Any]] = if (rows.length > 0) {
      rows.map(_.getValuesMap(columns))
    } else Nil

    val metrics = flattenMetric(metricMaps, name, flattenType)

    HashMap[String, Any](
      _MetricName -> name,
      _ApplicationID -> SparkSessionFactory.getInstance.sparkContext.applicationId,
      _BatchID -> batchId,
      _MetricValue -> metrics)
  }

  private def hasMultipleSinks(context: DQContext): Boolean = {
    context.getSinks.size > 1
  }

  private def flushBatch(
      context: DQContext,
      sink: Sink,
      batchDf: DataFrame,
      batchId: Long): Unit = {
    val multipleSinks = hasMultipleSinks(context)
    if (multipleSinks) batchDf.persist()

    val metrics: Map[String, Any] = processBatch(batchDf, batchId)
    sink.sinkBatchMetrics(metrics)

    if (multipleSinks) batchDf.unpersist()
  }

  private def flushMetrics(context: DQContext): Unit = {
    val df = context.sparkSession.table(s"`$inputName`")

    context.getSinks.foreach {
      case sink: Sink =>
        if (df.isStreaming) {
          val metricStreamingQuery = df.writeStream
            .queryName(s"metrics:$name:${sink.sinkParam.getName}")
            .outputMode(sink.sinkParam.getStreamingOutputMode)
            .trigger(sink.sinkParam.getTrigger)
            .foreachBatch((batchDf, batchId) => flushBatch(context, sink, batchDf, batchId))
            .start()

          StreamingQueryRegister.registerQuery(metricStreamingQuery)
        } else {
          flushBatch(context, sink, df, 0L)
        }
      case sink =>
        val errorMsg =
          s"Sink with name '${sink.sinkParam.getName}' does not support metric writing."
        warn(errorMsg)
    }
  }

  private def flattenMetric(
      metrics: Seq[Map[String, Any]],
      name: String,
      flattenType: FlattenType): HashMap[String, Any] = {
    flattenType match {
      case EntriesFlattenType => metrics.headOption.getOrElse(emptyMap)
      case ArrayFlattenType => HashMap[String, Any](name -> metrics)
      case MapFlattenType =>
        val v = metrics.headOption.getOrElse(emptyMap)
        HashMap[String, Any](name -> v)
      case _ =>
        if (metrics.size > 1) HashMap[String, Any](name -> metrics)
        else metrics.headOption.getOrElse(emptyMap)
    }
  }

}
