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

package org.apache.griffin.measure.configuration.dqdefinition

import com.fasterxml.jackson.annotation.{JsonInclude, JsonProperty}
import com.fasterxml.jackson.annotation.JsonInclude.Include
import io.netty.util.internal.StringUtil
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

/**
 * Model for Environment Config.
 *
 * @param sparkParam Job specific Spark Configs to override the Defaults set on the cluster
 * @param sinkParams A [[Seq]] of sink definitions where records and metrics can be persisted
 */
@JsonInclude(Include.NON_NULL)
case class EnvConfig(
    @JsonProperty("spark") private val sparkParam: SparkParam,
    @JsonProperty("sinks") private val sinkParams: List[SinkParam])
    extends Param {
  def getSparkParam: SparkParam = sparkParam
  def getSinkParams: Seq[SinkParam] = if (sinkParams != null) sinkParams else Nil

  def validate(): Unit = {
    assert(sparkParam != null, "spark param should not be null")
    sparkParam.validate()
    getSinkParams.foreach(_.validate())
    val repeatedSinks = sinkParams
      .map(_.getName)
      .groupBy(x => x)
      .mapValues(_.size)
      .filter(_._2 > 1)
      .keys
    assert(
      repeatedSinks.isEmpty,
      s"sink names must be unique. duplicate sink names ['${repeatedSinks.mkString("', '")}'] were found.")
  }
}

/**
 * spark param
 * @param logLevel         log level of spark application (optional)
 * @param config           extra config for spark environment (optional)
 */
@JsonInclude(Include.NON_NULL)
case class SparkParam(
    @JsonProperty("log.level") private val logLevel: String,
    @JsonProperty("config") private val config: Map[String, String])
    extends Param {
  def getLogLevel: String = if (logLevel != null) logLevel else "WARN"
  def getConfig: Map[String, String] = if (config != null) config else Map[String, String]()

  override def validate(): Unit = {}
}

/**
 * sink param
 * @param sinkType       sink type, e.g.: log, hdfs, http, mongo (must)
 * @param config         config of sink way (must)
 */
@JsonInclude(Include.NON_NULL)
case class SinkParam(
    @JsonProperty("name") private val name: String,
    @JsonProperty("type") private val sinkType: String,
    @JsonProperty("isStreaming") private val isStreaming: Boolean = false,
    @JsonProperty("streamingOutputMode") private val streamingOutputMode: String =
      OutputMode.Update().toString,
    @JsonProperty("triggerMs") private val triggerValue: Long = 0L,
    @JsonProperty("config") private val config: Map[String, Any] = Map.empty)
    extends Param {

  def getName: String = name
  def getType: String = sinkType
  def getConfig: Map[String, Any] = config
  def getIsStreaming: Boolean = isStreaming
  def getTrigger: Trigger = Trigger.ProcessingTime(triggerValue)
  def getStreamingOutputMode: String =
    if (StringUtil.isNullOrEmpty(streamingOutputMode)) OutputMode.Update().toString
    else streamingOutputMode

  def validate(): Unit = {
    assert(name != null, "sink name should must be defined")
    assert(StringUtils.isNotBlank(sinkType), "sink type should not be empty")
  }
}
