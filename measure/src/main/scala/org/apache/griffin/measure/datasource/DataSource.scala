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

import java.util.concurrent.atomic.AtomicLong

import scala.util.{Failure, Success}

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import org.apache.griffin.measure.Loggable
import org.apache.griffin.measure.configuration.dqdefinition.DataSourceParam
import org.apache.griffin.measure.context.DQContext
import org.apache.griffin.measure.execution.TableRegister
import org.apache.griffin.measure.execution.builder.DQJobBuilder
import org.apache.griffin.measure.step.builder.preproc.PreProcParamMaker
import org.apache.griffin.measure.utils.SparkSessionFactory

/**
 * Data source
 *
 * This abstracts the implementations of Batch and Streaming data sources.
 * Each implementation class must have a single constructor with exactly one parameter.
 * This parameter is a [[DataSourceParam]].
 *
 * `Connector` is a type definition that allows a custom data source to initialize external connection(s)
 *  objects which will be useful while reading data using `SparkSession`.
 *  For example: Before reading from MongoDB a user might want to persist some metadata in a store like MySQL.
 *  In this case, the devs can define say a Hikari JDBC Pool as a connector by overriding the `initializeConnector()`.
 *
 * @example {{{
 *          class MyDataSource(dataSourceParam: DataSourceParam) extends DataSource { ... }
 * }}}
 */
trait DataSource extends Loggable {
  type T
  type Connector

  val dataSourceParam: DataSourceParam

  val sparkSession: SparkSession = SparkSessionFactory.getInstance
  private val connector: Connector = initializeConnector()

  /**
   * Ability to optionally create external connections.
   * To override type `Connector` and `initializeConnector()` must be overridden. See example below,
   *
   * @example {{{
   *          class MyDataSource(dataSourceParam: DataSourceParam) extends DataSource {
   *                override type Connector = HikariDataSource
   *
   *                override def initializeConnector(): Connector = {
   *                    // Implementation for Connector
   *                    val myConn: HikariDataSource = ???
   *                    return myConn
   *                }
   *
   *                override read(): Dataset[T] {
   *                    // User connector
   *
   *                    val connector = getConnector
   *
   *                    ...
   *                }
   *          }
   * }}}
   * @return
   */
  protected def initializeConnector(): Connector

  /**
   * Gets the instance of `Connector`.
   * @return instance of `Connector`
   */
  def getConnector: Connector = connector

  /**
   * Reads the external data source as a spark `Dataset`. This `Dataset` may be batch or streaming.
   *
   * @return `Dataset` of `T` type objects. For example
   */
  def read(context: DQContext): DataFrame

  /**
   * Perform validations on provided `DataSourceParam` which holds user defined data source configurations.
   */
  def validate(): Unit = {}

  def preProcess(context: DQContext, ds: Dataset[T]): DataFrame = {
    val suffix = context.contextId.id
    val dcDfName = dataSourceParam.getDataFrameName("this")
    val (preProcRules, thisTable) =
      PreProcParamMaker.makePreProcRules(dataSourceParam.getPreProcRules, suffix, dcDfName)

    TableRegister.registerTable(thisTable, ds)

    // build job
    val preProcJob = DQJobBuilder.buildDQJob(context, preProcRules)

    // job execute
    preProcJob.execute(context) match {
      case Success(_) =>
      case Failure(exception) =>
        error("Exception occurred while applying preprocessing rules.", exception)
        throw exception
    }

    // out data
    context.sparkSession.table(s"`$thisTable`")
  }
}

/**
 * Batch Data source
 *
 * This is an abstraction for batch data sources. Implementations must override `readBatch()`.
 * Optionally, in case of some custom implementation, `initializeConnector()` may also be
 * overridden to instantiate a connector.
 */
trait BatchDataSource extends DataSource {

  /**
   * Reads the external data source as a spark `Dataset`.
   * This `Dataset` must not be streaming.
   *
   * Use `read()` of `SparkSession`.
   * @example {{{
   *            val sparkSession: SparkSession = ???
   *            sparkSession.read. ... . load()
   * }}}
   *
   * @return `Dataset` of `T` type objects. For example
   */
  def readBatch(): Dataset[T]

  override def read(context: DQContext): DataFrame = {
    val dataset = readBatch()
    preProcess(context, dataset)
  }
}

/**
 * Streaming Data source
 *
 * This is an abstraction for batch data sources. Implementations must override `readStream()`.
 * Optionally, in case of some custom implementation, `initializeConnector()` may also be
 * overridden to instantiate a connector.
 */
trait StreamingDataSource extends DataSource {

  /**
   * Reads the external data source as a spark `Dataset` using structured streaming.
   * This `Dataset` must streaming.
   *
   * Use `readStream()` of `SparkSession`.
   * @example {{{
   *            val sparkSession: SparkSession = ???
   *            sparkSession.readStream. ... . load()
   * }}}
   *
   * @return `Dataset` of `T` type objects. For example
   */
  def readStream(): Dataset[T]

  override def read(context: DQContext): DataFrame = {
    val dataset = readStream()
    assert(dataset.isStreaming, "Dataset is not streaming.")

    preProcess(context, dataset)
  }
}

object DataSource {

  private val counter: AtomicLong = new AtomicLong(0L)
  private val head: String = "dc"

  def genId: String = {
    s"$head$increment"
  }

  private def increment: Long = {
    counter.incrementAndGet()
  }
}
