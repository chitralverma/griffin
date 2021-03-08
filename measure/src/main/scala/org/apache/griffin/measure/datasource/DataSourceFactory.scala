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

import scala.util.{Failure, Success, Try}

import org.apache.griffin.measure.Loggable
import org.apache.griffin.measure.SourceSinkConstants._
import org.apache.griffin.measure.configuration.dqdefinition.DataSourceParam
import org.apache.griffin.measure.datasource.impl._
import org.apache.griffin.measure.utils.CommonUtils

/**
 * DataSourceFactory
 *
 * This code allows instantiation of [[DataSource]]s (Batch or Streaming).
 */
object DataSourceFactory extends Loggable {

  def getDataSources(dataSources: Seq[DataSourceParam]): Seq[DataSource] = {
    dataSources.map((param: DataSourceParam) => getDataSource(param))
  }

  /**
   * Instantiates a Data Source (batch or streaming based on user defined configuration).
   *
   * @param dataSourceParam data source param which holds the user defined configuration
   * @return [[DataSource]] instance
   */
  private def getDataSource(dataSourceParam: DataSourceParam): DataSource = {
    Try {
      val cls = getDataSourceClass(dataSourceParam)

      if (dataSourceParam.getIsStreaming) {
        if (classOf[StreamingDataSource].isAssignableFrom(cls)) {
          getDataSourceInstance(cls, dataSourceParam)
        } else {
          val errorMsg =
            s"Data Source with class name '${cls.getCanonicalName}' does not support streaming."
          val exception = new IllegalStateException(errorMsg)
          throw exception
        }
      } else if (classOf[BatchDataSource].isAssignableFrom(cls)) {
        getDataSourceInstance(cls, dataSourceParam)
      } else {
        val errorMsg =
          s"Class name '${cls.getCanonicalName}' cannot be instantiated as a valid Batch or Streaming data source."
        val exception = new ClassCastException(errorMsg)
        throw exception
      }
    } match {
      case Success(dataSource) => dataSource
      case Failure(exception) =>
        error(
          s"""Exception occurred while getting data source with name '${dataSourceParam.getName}'
             |and type '${dataSourceParam.getType}'""".stripMargin,
          exception)
        throw exception
    }
  }

  /**
   * Returns a Data Source class corresponding to the type defined in the configuration.
   * If type does not match any of the internal types (regexes), then its assumed to be class name.
   *
   * [Regex -> Class] mapping of all internally supported data sources must be added as a `case`.
   *
   * Note: These classes must exist on Griffin Class path. To do this users can set
   * `spark.jars` in spark config section of env config.
   *
   * @param dataSourceParam data source param
   * @return [[Class]] of the data source
   */
  private def getDataSourceClass(dataSourceParam: DataSourceParam): Class[_] = {
    val dataSourceType = dataSourceParam.getType

    Try {
      dataSourceType match {
        case HiveRegex() => classOf[HiveDataSource]
        case FileRegex() => classOf[FileDataSource]
        case ElasticSearchRegex() => classOf[ElasticsearchDataSource]
        case JDBCRegex() => classOf[JDBCDataSource]
        case CassandraRegex() => classOf[CassandraDataSource]
        case className => Class.forName(className, true, CommonUtils.getContextOrMainClassLoader)
      }
    } match {
      case Failure(e: ClassNotFoundException) =>
        error(
          s"Class for data source of type '$dataSourceType' was not found on the classpath.",
          e)
        throw e
      case Failure(exception) =>
        error(s"Unable to initialize data source of type '$dataSourceType'", exception)
        throw exception
      case Success(cls) => cls
    }
  }

  /**
   * Creates an instance of the data source based on the class.
   *
   * @throws ClassCastException when the provided class name does not extend [[DataSource]]
   * @param dataSourceParam data source param
   * @param cls [[Class]] of the data source
   * @return A new instance of the data source
   */
  private def getDataSourceInstance(
      cls: Class[_],
      dataSourceParam: DataSourceParam): DataSource = {
    debug(s"Attempting to instantiate class with name ${cls.getCanonicalName}")

    Try(
      cls
        .getConstructor(classOf[DataSourceParam])
        .newInstance(dataSourceParam)
        .asInstanceOf[DataSource]) match {
      case Success(c) =>
        info(s"Successfully instantiated data source with name '${dataSourceParam.getName}'")
        c
      case Failure(exception) =>
        error(
          s"Error encountered while instantiating data source with name '${dataSourceParam.getName}'",
          exception)
        throw exception
    }
  }

}
