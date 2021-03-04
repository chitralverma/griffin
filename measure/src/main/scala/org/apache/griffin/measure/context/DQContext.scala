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

package org.apache.griffin.measure.context

import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession}

import org.apache.griffin.measure.configuration.dqdefinition._
import org.apache.griffin.measure.configuration.enums.ProcessType._
import org.apache.griffin.measure.configuration.enums.WriteMode
import org.apache.griffin.measure.datasource._
import org.apache.griffin.measure.execution.TableRegister
import org.apache.griffin.measure.sink.{Sink, SinkFactory}
import org.apache.griffin.measure.utils.SparkSessionFactory

/**
 * dq context: the context of each calculation
 * unique context id in each calculation
 * access the same spark session this app created
 */
case class DQContext(
    contextIdOpt: Option[ContextId] = None,
    applicationName: String,
    dataSources: Seq[DataSource],
    sinkParams: Seq[SinkParam],
    procType: ProcessType) {

  val sparkSession: SparkSession = SparkSessionFactory.getInstance

  val contextId: ContextId = contextIdOpt match {
    case Some(x) => x
    case None => ContextId(System.currentTimeMillis)
  }

  val dataFrameCache: DataFrameCache = DataFrameCache()

  val metricWrapper: MetricWrapper =
    MetricWrapper(applicationName, sparkSession.sparkContext.applicationId)
  val writeMode: WriteMode = WriteMode.defaultMode(procType)

  val dataSourceNames: Seq[String] = {
    // sort data source names, put baseline data source name to the head
    val (blOpt, others) = dataSources.foldLeft((None: Option[String], Nil: Seq[String])) {
      (ret, ds) =>
        val dsName = ds.dataSourceParam.getName
        val (opt, seq) = ret
        if (opt.isEmpty) (Some(dsName), seq) else (opt, seq :+ dsName)
    }
    blOpt match {
      case Some(bl) => bl +: others
      case _ => others
    }
  }

  def getDataSourceName(index: Int): String = {
    if (dataSourceNames.size > index) dataSourceNames(index) else ""
  }

  implicit val encoder: Encoder[String] = Encoders.STRING
  val functionNames: Seq[String] = sparkSession.catalog.listFunctions.map(_.name).collect.toSeq

  val dataSourceTimeRanges: Map[String, DataFrame] = loadDataSources()

  def loadDataSources(): Map[String, DataFrame] = {
    dataSources.map { ds =>
      {
        ds.validate()
        val dsName = ds.dataSourceParam.getName
        val df = ds.read(this)

        TableRegister.registerTable(dsName, df)
        (dsName, df)
      }
    }.toMap
  }

  private val sinkFactory = SinkFactory(sinkParams, applicationName)
  private val defaultSinks: Seq[Sink] = createSinks(contextId.timestamp)

  def getSinks(timestamp: Long): Seq[Sink] = {
    if (timestamp == contextId.timestamp) getSinks
    else createSinks(timestamp)
  }

  def getSinks: Seq[Sink] = defaultSinks

  private def createSinks(t: Long): Seq[Sink] = {
    procType match {
      case BatchProcessType => sinkFactory.getSinks(t, block = true)
      case StreamingProcessType => sinkFactory.getSinks(t, block = false)
    }
  }

  def clean(): Unit = {
    dataFrameCache.uncacheAllDataFrames()
    dataFrameCache.clearAllTrashDataFrames()
  }

}
