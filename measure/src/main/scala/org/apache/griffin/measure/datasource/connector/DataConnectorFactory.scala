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

package org.apache.griffin.measure.datasource.connector

import scala.util.Try
import scala.util.matching.Regex

import org.apache.spark.sql.SparkSession

import org.apache.griffin.measure.Loggable
import org.apache.griffin.measure.configuration.dqdefinition.DataConnectorParam
import org.apache.griffin.measure.datasource.TimestampStorage
import org.apache.griffin.measure.datasource.cache.StreamingCacheClient
import org.apache.griffin.measure.datasource.connector.batch._
import org.apache.griffin.measure.datasource.connector.streaming.StreamingDataConnector

object DataConnectorFactory extends Loggable {

  @deprecated val AvroRegex: Regex = """^(?i)avro$""".r
  @deprecated val TextDirRegex: Regex = """^(?i)text-dir$""".r

  val HiveRegex: Regex = """^(?i)hive$""".r
  val FileRegex: Regex = """^(?i)file$""".r
  val KafkaRegex: Regex = """^(?i)kafka$""".r
  val JDBCRegex: Regex = """^(?i)jdbc$""".r
  val CustomRegex: Regex = """^(?i)custom$""".r
  val ElasticSearchRegex: Regex = """^(?i)elasticsearch$""".r

  /**
   * create data connector
   * @param sparkSession     spark env
   * @param dcParam          data connector param
   * @param tmstCache        same tmst cache in one data source
   * @param streamingCacheClientOpt   for streaming cache
   * @return   data connector
   */
  def getDataConnector(
      sparkSession: SparkSession,
      dcParam: DataConnectorParam,
      tmstCache: TimestampStorage,
      streamingCacheClientOpt: Option[StreamingCacheClient]): Try[DataConnector] = {
    val conType = dcParam.getType
    Try {
      conType match {
        case HiveRegex() => HiveBatchDataConnector(sparkSession, dcParam, tmstCache)
        case FileRegex() => FileBasedDataConnector(sparkSession, dcParam, tmstCache)
        case ElasticSearchRegex() => ElasticSearchDataConnector(sparkSession, dcParam, tmstCache)
        case CustomRegex() =>
          getCustomConnector(sparkSession, dcParam, tmstCache, streamingCacheClientOpt)
        case JDBCRegex() => JDBCBasedDataConnector(sparkSession, dcParam, tmstCache)
        case AvroRegex() => AvroBatchDataConnector(sparkSession, dcParam, tmstCache)
        case TextDirRegex() => TextDirBatchDataConnector(sparkSession, dcParam, tmstCache)
        case _ => throw new Exception("connector creation error!")
      }
    }
  }

  private def getCustomConnector(
      sparkSession: SparkSession,
      dcParam: DataConnectorParam,
      timestampStorage: TimestampStorage,
      streamingCacheClientOpt: Option[StreamingCacheClient]): DataConnector = {
    val className = dcParam.getConfig("class").asInstanceOf[String]
    val cls = Class.forName(className)
    if (classOf[BatchDataConnector].isAssignableFrom(cls)) {
      val method = cls.getDeclaredMethod(
        "apply",
        classOf[SparkSession],
        classOf[DataConnectorParam],
        classOf[TimestampStorage])
      method
        .invoke(null, sparkSession, dcParam, timestampStorage)
        .asInstanceOf[BatchDataConnector]
    } else if (classOf[StreamingDataConnector].isAssignableFrom(cls)) {
      val method = cls.getDeclaredMethod(
        "apply",
        classOf[SparkSession],
        classOf[DataConnectorParam],
        classOf[TimestampStorage],
        classOf[Option[StreamingCacheClient]])
      method
        .invoke(null, sparkSession, dcParam, timestampStorage, streamingCacheClientOpt)
        .asInstanceOf[StreamingDataConnector]
    } else {
      throw new ClassCastException(
        s"$className should extend BatchDataConnector or StreamingDataConnector")
    }
  }

}
