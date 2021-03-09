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

import scala.util._

import org.apache.spark.sql.AnalysisException

import org.apache.griffin.measure.configuration.dqdefinition.{AppConfig, EnvConfig, GriffinConfig}
import org.apache.griffin.measure.configuration.dqdefinition.reader.ParamReaderFactory
import org.apache.griffin.measure.sink.CustomSinkResultRegister
import org.apache.griffin.measure.SparkSuiteBase

class BatchGriffinJobExecutorTest extends SparkSuiteBase {

  val envParam: EnvConfig =
    ParamReaderFactory.readParam[EnvConfig](getConfigFilePath("/env-batch.json"))

  override def beforeEach(): Unit = {
    super.beforeEach()
    CustomSinkResultRegister.clear()
  }

  private def getConfigFilePath(fileName: String): String = {
    try {
      getClass.getResource(fileName).getFile
    } catch {
      case _: NullPointerException => throw new Exception(s"resource [$fileName] not found")
      case ex: Throwable => throw ex
    }
  }

  private def getGriffinJobExecutor(dqParamFile: String): GriffinJobExecutor = {
    val appConfig = ParamReaderFactory.readParam[AppConfig](getConfigFilePath(dqParamFile))
    val griffinConfig = GriffinConfig(envParam, appConfig)

    GriffinJobExecutor(griffinConfig)
  }

  def runAndCheckResult(executor: GriffinJobExecutor, metrics: Map[String, Any]): Unit = {
    val executionResult: Try[Boolean] = executor.execute()
    assert(executionResult.isSuccess, "Job execution failed.")

    griffinLogger.info(metrics)

    //    check Result Metrics
    val actualMetrics =
      CustomSinkResultRegister.getMetrics("customSink").flatMap(_.get("metricValue"))

    assert(actualMetrics.isDefined)
    actualMetrics.get should equal(metrics)
  }

  def runAndCheckException(executor: GriffinJobExecutor, cls: Class[_]): Unit = {
    val executionResult: Try[Boolean] = executor.execute()

    executionResult match {
      case Failure(exception) =>
        assert(exception.getClass == cls, "Un expected exception was thrown.")
      case Success(_) =>
        assert(
          executionResult.isFailure,
          s"Job ${executor.appConfig.getName} should not succeed.")
    }
  }

  "accuracy batch job" should "work" in {
    val dqParamFile = "/_accuracy-batch-griffindsl.json"
    val executor = getGriffinJobExecutor(dqParamFile)

    val expectedMetrics = Map(
      "total_count" -> 50,
      "miss_count" -> 4,
      "matched_count" -> 46,
      "matchedFraction" -> 0.92)

    runAndCheckResult(executor, expectedMetrics)
  }

  "completeness batch job" should "work" in {
    val dqParamFile = "/_completeness-batch-griffindsl.json"
    val executor = getGriffinJobExecutor(dqParamFile)

    val expectedMetrics = Map("total" -> 50, "incomplete" -> 1, "complete" -> 49)

    runAndCheckResult(executor, expectedMetrics)
  }

  "distinctness batch job" should "work" in {
    val dqParamFile = "/_distinctness-batch-griffindsl.json"
    val executor = getGriffinJobExecutor(dqParamFile)

    val expectedMetrics =
      Map("dup" -> Seq(Map("dup" -> 1, "num" -> 1)))

    runAndCheckResult(executor, expectedMetrics)
  }

  "profiling batch job" should "work" in {
    val dqParamFile = "/_profiling-batch-griffindsl.json"
    val executor = getGriffinJobExecutor(dqParamFile)

    val expectedMetrics = Map("post_group" -> Seq(Map("post_code" -> "94022", "cnt" -> 13)))

    runAndCheckResult(executor, expectedMetrics)
  }

  "timeliness batch job" should "work" in {
    val dqParamFile = "/_timeliness-batch-griffindsl.json"
    val executor = getGriffinJobExecutor(dqParamFile)

    val expectedMetrics = Map("percentile_95" -> 660000)

    runAndCheckResult(executor, expectedMetrics)
  }

  "uniqueness batch job" should "work" in {
    val dqParamFile = "/_uniqueness-batch-griffindsl.json"
    val executor = getGriffinJobExecutor(dqParamFile)

    val expectedMetrics = Map("unique" -> 48)

    runAndCheckResult(executor, expectedMetrics)
  }

  "batch job" should "fail with exception caught due to invalid rules" in {
    val dqParamFile = "/_profiling-batch-griffindsl_malformed.json"
    val executor = getGriffinJobExecutor(dqParamFile)

    runAndCheckException(executor, classOf[AnalysisException])
  }
}
