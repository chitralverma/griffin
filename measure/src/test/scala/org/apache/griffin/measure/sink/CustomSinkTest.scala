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

import scala.util._

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._

import org.apache.griffin.measure.configuration.dqdefinition._
import org.apache.griffin.measure.configuration.enums.FlattenType.DefaultFlattenType
import org.apache.griffin.measure.step.write.{MetricWriteStep, RecordWriteStep}
import org.apache.griffin.measure.SparkSuiteBase
import org.apache.griffin.measure.context.{ContextId, DQContext}

class CustomSinkTest extends SparkSuiteBase {

  private val appConfig: AppConfig =
    AppConfig(name = "CustomSinkTestApp", dataSources = Nil, evaluateRule = null, sinks = Nil)

  // Sink Params
  private val metricSinkParam: SinkParam =
    SinkParam(name = "customMetricSink", sinkType = classOf[CustomMetricSink].getCanonicalName)
  private val batchSinkParam: SinkParam =
    SinkParam(name = "customBatchSink", sinkType = classOf[CustomBatchSink].getCanonicalName)

  private val sinkParams: Seq[SinkParam] = Seq(metricSinkParam, batchSinkParam)

  var dQContext: DQContext = _

  // Sinks
  private var metricSink: MetricSink = _
  private var batchSink: BatchSink = _

  val metricsDefaultOutput: RuleOutputParam =
    RuleOutputParam("metrics", "default_output", "default")

  override def beforeAll(): Unit = {
    super.beforeAll()
    dQContext =
      DQContext(Some(ContextId(System.currentTimeMillis)), appConfig = appConfig, Nil, sinkParams)
    metricSink =
      dQContext.getSinks.find(_.sinkParam == metricSinkParam).get.asInstanceOf[MetricSink]
    batchSink = dQContext.getSinks.find(_.sinkParam == batchSinkParam).get.asInstanceOf[BatchSink]
  }

  def createDataFrame(arr: Seq[Int]): DataFrame = {
    val schema = StructType(
      Array(
        StructField("id", LongType),
        StructField("name", StringType),
        StructField("sex", StringType),
        StructField("age", IntegerType)))
    val rows = arr.map { i =>
      Row(i.toLong, s"name_$i", if (i % 2 == 0) "man" else "women", i + 15)
    }

    val rowRdd = spark.sparkContext.parallelize(rows)
    spark.createDataFrame(rowRdd, schema)
  }

  override def beforeEach(): Unit = {
    CustomSinkResultRegister.clear()
  }

  "custom sink" can "sink metrics" in {
    val sinkMetrics1Result = Try(metricSink.sinkMetrics(Map("sum" -> 10))).isSuccess
    assert(sinkMetrics1Result)

    val sinkMetrics2Result = Try(metricSink.sinkMetrics(Map("count" -> 5))).isSuccess
    assert(sinkMetrics2Result)

    val actualMetrics = CustomSinkResultRegister.getMetrics(metricSink.sinkParam.getName)

    val expected = Map("sum" -> 10, "count" -> 5)
    assert(actualMetrics.isDefined)
    actualMetrics.get should contain theSameElementsAs expected
  }

  "custom sink" can "sink records" in {
    val df1 = createDataFrame(1 to 2)
    val sinkMetrics1Result = Try(batchSink.sinkBatchRecords("testMeasure", df1)).isSuccess
    assert(sinkMetrics1Result)

    val df2 = createDataFrame(2 to 4)
    val sinkMetrics2Result = Try(batchSink.sinkBatchRecords("testMeasure", df2)).isSuccess
    assert(sinkMetrics2Result)

    val actualBatch: Option[Array[String]] =
      CustomSinkResultRegister.getBatch(batchSink.sinkParam.getName)

    val expected = List(
      "{\"id\":1,\"name\":\"name_1\",\"sex\":\"women\",\"age\":16}",
      "{\"id\":2,\"name\":\"name_2\",\"sex\":\"man\",\"age\":17}",
      "{\"id\":2,\"name\":\"name_2\",\"sex\":\"man\",\"age\":17}",
      "{\"id\":3,\"name\":\"name_3\",\"sex\":\"women\",\"age\":18}",
      "{\"id\":4,\"name\":\"name_4\",\"sex\":\"man\",\"age\":19}")

    assert(actualBatch.isDefined)
    actualBatch.get should contain theSameElementsAs expected
  }

  "MetricWriteStep" should "output default metrics with custom sink" in {
    val resultTable = "result_table"
    val df = createDataFrame(1 to 5)
    df.groupBy("sex")
      .agg("age" -> "max", "age" -> "avg")
      .createOrReplaceTempView(resultTable)

    val metricWriteStep = {
      val metricOpt = Some(metricsDefaultOutput)
      val mwName = metricOpt.flatMap(_.getNameOpt).getOrElse("default_metrics_name")
      val flattenType = metricOpt.map(_.getFlatten).getOrElse(DefaultFlattenType)
      MetricWriteStep(mwName, resultTable, flattenType)
    }

    metricWriteStep.execute(dQContext)

    val actualMetrics = CustomSinkResultRegister.getMetrics(metricSink.sinkParam.getName)

    val metricsValue = Seq(
      Map("sex" -> "man", "max(age)" -> 19, "avg(age)" -> 18.0),
      Map("sex" -> "women", "max(age)" -> 20, "avg(age)" -> 18.0))

    val expected = Map("default_output" -> metricsValue)

    assert(actualMetrics.isDefined)
    actualMetrics.get("metricValue") should be(expected)
  }

  "RecordWriteStep" should "work with custom sink" in {
    val resultTable = "result_table"
    val df = createDataFrame(1 to 5)
    df.createOrReplaceTempView(resultTable)

    val rwName = Some(metricsDefaultOutput).flatMap(_.getNameOpt).getOrElse(resultTable)
    RecordWriteStep(rwName, resultTable).execute(dQContext)

    val actualBatch = CustomSinkResultRegister.getBatch(batchSink.sinkParam.getName)

    val expected = List(
      "{\"id\":1,\"name\":\"name_1\",\"sex\":\"women\",\"age\":16}",
      "{\"id\":2,\"name\":\"name_2\",\"sex\":\"man\",\"age\":17}",
      "{\"id\":3,\"name\":\"name_3\",\"sex\":\"women\",\"age\":18}",
      "{\"id\":4,\"name\":\"name_4\",\"sex\":\"man\",\"age\":19}",
      "{\"id\":5,\"name\":\"name_5\",\"sex\":\"women\",\"age\":20}")

    assert(actualBatch.isDefined)
    actualBatch.get should contain theSameElementsAs expected
  }

}
