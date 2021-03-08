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

import scala.collection.mutable

import org.apache.spark.sql.DataFrame

import org.apache.griffin.measure.configuration.dqdefinition.{AppConfig, SinkParam}

/**
 * Register for storing test sink results in memory
 */
object CustomSinkResultRegister {

  private val _metricsSink: mutable.Map[String, Map[String, Any]] = mutable.HashMap.empty
  private val _batchSink: mutable.Map[String, Array[String]] = mutable.HashMap.empty

  def setMetrics(key: String, metrics: Map[String, Any]): Unit = {
    val updatedMetrics = _metricsSink.getOrElse(key, Map.empty) ++ metrics
    _metricsSink.put(key, updatedMetrics)
  }

  def getMetrics(key: String): Option[Map[String, Any]] = _metricsSink.get(key)

  def setBatch(key: String, batch: Array[String]): Unit = {
    val updatedBatch = _batchSink.getOrElse(key, Array.empty) ++ batch
    _batchSink.put(key, updatedBatch)
  }

  def getBatch(key: String): Option[Array[String]] = _batchSink.get(key)

  def clear(): Unit = {
    _metricsSink.clear()
    _batchSink.clear()
  }

}

/**
 * A dummy batch sink for testing
 * @param appConfig Application Config
 * @param sinkParam Sink definition
 */
class CustomBatchSink(val appConfig: AppConfig, val sinkParam: SinkParam) extends BatchSink {

  override def sinkBatchRecords(measureName: String, dataset: DataFrame): Unit = {
    CustomSinkResultRegister.setBatch(sinkParam.getName, dataset.toJSON.collect())
  }
}

/**
 * A dummy metric sink for testing
 * @param appConfig Application Config
 * @param sinkParam Sink definition
 */
class CustomMetricSink(val appConfig: AppConfig, val sinkParam: SinkParam) extends MetricSink {

  override def sinkMetrics(metrics: Map[String, Any]): Unit = {
    CustomSinkResultRegister.setMetrics(sinkParam.getName, metrics)
  }
}
