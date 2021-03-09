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

package org.apache.griffin.measure.datasource

import scala.util._

import org.apache.spark.sql.{Dataset, Row}

import org.apache.griffin.measure.SparkSuiteBase
import org.apache.griffin.measure.configuration.dqdefinition.DataSourceParam
import org.apache.griffin.measure.datasource.impl.FileDataSource

class ExampleBatchDataSource(val dataSourceParam: DataSourceParam) extends BatchDataSource {

  override type T = Row
  override type Connector = Unit

  override def readBatch(): Dataset[T] = sparkSession.emptyDataFrame
  override protected def initializeConnector(): Connector = {}
}

class NotADataSource

class DataSourceFactorySpec extends SparkSuiteBase {

  "DataSourceFactory" should "allow custom type" in {
    val param = DataSourceParam(
      name = "custom datasource",
      conType = classOf[ExampleBatchDataSource].getCanonicalName,
      config = Map.empty,
      dataFrameName = null,
      preProc = Nil)

    val method = PrivateMethod[DataSource]('getDataSource)
    val res = Try(DataSourceFactory.invokePrivate(method(param)))

    assert(res.isSuccess)
    assert(res.get != null)
    assert(res.get.isInstanceOf[ExampleBatchDataSource])
    assert(res.get.asInstanceOf[ExampleBatchDataSource].readBatch().collect().length == 0)
  }

  it should "be able to create FileDataSource" in {

    val configs = Map(
      "format" -> "csv",
      "paths" -> Seq(s"file://${getClass.getResource("/hive/person_table.csv").getPath}"),
      "options" -> Map("header" -> "false"))

    val param = DataSourceParam(
      name = "file datasource",
      conType = "file",
      dataFrameName = "test_df",
      config = configs,
      preProc = Nil)

    val method = PrivateMethod[DataSource]('getDataSource)
    val res = Try(DataSourceFactory.invokePrivate(method(param)))

    assert(res.isSuccess)
    assert(res.get != null)
    assert(res.get.isInstanceOf[FileDataSource])
    assert(res.get.asInstanceOf[FileDataSource].readBatch().collect().length == 2)
  }

  it should "fail if provided class is not a valid DataSource" in {
    val param = DataSourceParam(
      name = "custom datasource",
      conType = classOf[NotADataSource].getCanonicalName,
      config = Map.empty,
      dataFrameName = null,
      preProc = Nil)

    val method = PrivateMethod[DataSource]('getDataSource)
    val res = Try(DataSourceFactory.invokePrivate(method(param)))

    assert(res.isFailure)
    assert(res.failed.get.isInstanceOf[ClassCastException])
    assert(
      res.failed.get.getMessage ==
        s"Class name '${classOf[NotADataSource].getCanonicalName}' cannot be instantiated " +
          s"as a valid Batch or Streaming data source.")
  }

}
