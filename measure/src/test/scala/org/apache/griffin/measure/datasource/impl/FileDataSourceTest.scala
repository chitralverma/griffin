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

import scala.util._

import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

import org.apache.griffin.measure.SparkSuiteBase
import org.apache.griffin.measure.configuration.dqdefinition.DataSourceParam

class FileDataSourceTest extends SparkSuiteBase {

  private final val dcParam = DataSourceParam(
    name = "datasource",
    conType = "file",
    dataFrameName = "test_df",
    config = Map.empty,
    preProc = Nil)

  "file based data source" should "be able to read csv files from local filesystem" in {
    val configs = Map(
      "format" -> "csv",
      "paths" -> Seq(s"file://${getClass.getResource("/hive/person_table.csv").getPath}"),
      "options" -> Map("header" -> "false"))

    val dc = new FileDataSource(dcParam.copy(config = configs))
    val result = Try(dc.readBatch()).toOption

    assert(result.isDefined)
    assert(result.get.collect().length == 2)
  }

  // Regarding User Defined Schema

  it should "respect the provided schema, if any" in {
    val configs = Map(
      "format" -> "csv",
      "paths" -> Seq(s"file://${getClass.getResource("/hive/person_table.csv").getPath}"))

    // no schema
    assertThrows[IllegalArgumentException](
      new FileDataSource(dcParam.copy(config = configs)).validate())

    // invalid schema
    assertThrows[IllegalStateException](
      new FileDataSource(dcParam.copy(config = configs + (("schema", "")))).validate())

    // valid schema
    val schema = Seq(
      Map("name" -> "name", "type" -> "string"),
      Map("name" -> "age", "type" -> "int", "nullable" -> "true"))

    val result1 = Try(
      new FileDataSource(dcParam.copy(config = configs + (("schema", schema))))
        .readBatch()).toOption

    val expSchema = new StructType()
      .add("name", StringType)
      .add("age", IntegerType, nullable = true)

    assert(result1.isDefined)
    assert(result1.get.collect().length == 2)
    assert(result1.get.schema == expSchema)

    // valid headers
    val result2 = Try(
      new FileDataSource(dcParam.copy(config = configs + (("options", Map("header" -> "true")))))
        .readBatch()).toOption

    assert(result2.isDefined)
    assert(result2.get.collect().length == 1)
    result2.get.columns should contain theSameElementsAs Seq("Joey", "14")
  }

  // skip on erroneous paths

  it should "respect options if an erroneous path is encountered" in {
    val configs = Map(
      "format" -> "csv",
      "paths" -> Seq(
        s"file://${getClass.getResource("/hive/person_table.csv").getPath}",
        s"${java.util.UUID.randomUUID().toString}/"),
      "skipErrorPaths" -> true,
      "options" -> Map("header" -> "true"))

    // valid paths
    val result1 = Try(new FileDataSource(dcParam.copy(config = configs)).readBatch()).toOption
    assert(result1.isDefined)
    assert(result1.get.collect().length == 1)

    // non existent path
    assertThrows[IllegalArgumentException](
      new FileDataSource(dcParam.copy(config = configs - "skipErrorPaths")).validate())

    // no path
    assertThrows[AssertionError](
      new FileDataSource(dcParam.copy(config = configs - "paths")).validate())
  }

  // Regarding various formats
  it should "be able to read all supported file types" in {

    val formats = Seq("avro", "parquet", "orc", "csv", "tsv")

    formats.map(f => {
      val configs = Map(
        "format" -> f,
        "paths" -> Seq(s"file://${getClass.getResource(s"/files/person_table.$f").getPath}"),
        "options" -> Map("header" -> "true", "inferSchema" -> "true"))

      val result = {
        Try({
          val fds = new FileDataSource(dcParam.copy(config = configs))
          fds.validate()
          fds.readBatch()
        }).toOption

      }
      assert(result.isDefined)

      val df = result.get
      val expSchema = new StructType()
        .add("name", StringType)
        .add("age", IntegerType, nullable = true)

      assert(df.collect().length == 2)
      assert(df.schema == expSchema)
    })
  }

  it should "apply schema to all formats if provided" in {
    val formats = Seq("avro", "parquet", "orc", "csv", "tsv")
    formats.map(f => {
      val configs = Map(
        "format" -> f,
        "paths" -> Seq(
          s"file://${ClassLoader.getSystemResource(s"files/person_table.$f").getPath}"),
        "options" -> Map("header" -> "true"),
        "schema" -> Seq(Map("name" -> "name", "type" -> "string")))

      val result = {
        Try({
          val fds = new FileDataSource(dcParam.copy(config = configs))
          fds.validate()
          fds.readBatch()
        }).toOption
      }

      assert(result.isDefined)

      val df = result.get
      val expSchema = new StructType()
        .add("name", StringType)

      assert(df.collect().length == 2)
      assert(df.schema == expSchema)
    })
  }

}
