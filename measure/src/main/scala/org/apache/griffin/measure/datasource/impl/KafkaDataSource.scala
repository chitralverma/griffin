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

package org.apache.griffin.measure.datasource.impl

import scala.collection.mutable.{Map => MutableMap}

import org.apache.spark.sql.{Dataset, Row}

import org.apache.griffin.measure.configuration.dqdefinition.DataSourceParam
import org.apache.griffin.measure.datasource.{BatchDataSource, StreamingDataSource}
import org.apache.griffin.measure.utils.ParamUtil._

class KafkaDataSource(val dataSourceParam: DataSourceParam)
    extends BatchDataSource
    with StreamingDataSource {

  override type T = Row
  override type Connector = Unit

  override protected def initializeConnector(): Connector = {}

  private val Options: String = "options"

  val config: Map[String, Any] = dataSourceParam.getConfig
  val options: MutableMap[String, String] = MutableMap(
    config.getParamStringMap(Options, Map.empty).toSeq: _*)

  override def readBatch(): Dataset[T] = {
    sparkSession.read.format("kafka").options(options).load()
  }

  override def readStream(): Dataset[T] = {
    sparkSession.readStream.format("kafka").options(options).load()
  }

}
