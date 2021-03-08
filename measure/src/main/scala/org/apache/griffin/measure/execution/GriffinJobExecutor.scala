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

package org.apache.griffin.measure.execution

import java.util.concurrent.TimeUnit

import scala.util.Try

import org.apache.spark.metrics.sink.Sink
import org.apache.spark.sql.SparkSession

import org.apache.griffin.measure.Loggable
import org.apache.griffin.measure.configuration.dqdefinition._
import org.apache.griffin.measure.context.DQContext
import org.apache.griffin.measure.datasource.DataSourceFactory
import org.apache.griffin.measure.execution.builder.DQJobBuilder
import org.apache.griffin.measure.step.builder.udf.GriffinUDFAgent
import org.apache.griffin.measure.utils.{CommonUtils, SparkSessionFactory}

case class GriffinJobExecutor(griffinConfig: GriffinConfig) extends Loggable {

  val appConfig: AppConfig = griffinConfig.appConfig
  val envConfig: EnvConfig = griffinConfig.envConfig

  var dqContext: DQContext = _

  private def initializeSparkSession(): SparkSession = {
    val sparkParam = griffinConfig.envConfig.getSparkParam
    val sparkConfigMap: Map[String, String] =
      sparkParam.getConfig + ("spark.app.name" -> appConfig.getName) + ("spark.sql.crossJoin.enabled" -> "true")

    val sparkSession = SparkSessionFactory.create(sparkConfigMap)

    griffinLogger.setLevel(getGriffinLogLevel)
    sparkSession.sparkContext.setLogLevel(sparkParam.getLogLevel)

    GriffinUDFAgent.register(sparkSession)
    sparkSession
  }

  private def cleanup(dqContext: DQContext): Unit = {
    // close `Sink`s
    dqContext.getSinks.foreach(_.close())
  }

  def execute(): Try[Boolean] = {
    CommonUtils.timeThis({
      // initialize `SparkSession`
      val sparkSession = initializeSparkSession()

      // get data sources
      val dataSources = DataSourceFactory.getDataSources(appConfig.getDataSourceParams)

      // initialize DQ Context
      dqContext =
        DQContext(appConfig = appConfig, dataSources = dataSources, sinkParams = getSinkParams)

      // initialize `Sink`s
      dqContext.getSinks.foreach(_.open())

      // execute job
      val dqJob = DQJobBuilder.buildDQJob(dqContext, appConfig.getEvaluateRule)
      val result = dqJob.execute(dqContext)

      // await streams if any
      if (StreamingQueryRegister.getQueries.nonEmpty) {
        sparkSession.streams.awaitAnyTermination()
      }

      // perform clean up
      cleanup(dqContext)

      // return result
      result
    }, TimeUnit.MILLISECONDS)
  }

  /**
   * Gets a valid [[Sink]] definition from the Env Config for each [[Sink]] defined in Job Config.
   *
   * @throws AssertionError if Env Config does not contain definition for a sink defined in Job Config
   * @return [[Seq]] of [[Sink]] definitions
   */
  private def getSinkParams: Seq[SinkParam] = {
    val sinkParams = appConfig.getSinkNames
      .map(_.toLowerCase())
      .map { sinkName =>
        (sinkName, envConfig.getSinkParams.find(_.getName.toLowerCase().matches(sinkName)))
      }

    val missingSinks = sinkParams.filter(_._2.isEmpty).map(_._1)

    assert(
      missingSinks.isEmpty,
      s"Sink(s) ['${missingSinks.mkString("', '")}'] not defined in env config.")

    sinkParams.flatMap(_._2)
  }
}
