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

import scala.util.{Failure, Success, Try}

import org.apache.griffin.measure.Loggable
import org.apache.griffin.measure.SourceSinkConstants._
import org.apache.griffin.measure.configuration.dqdefinition.{AppConfig, SinkParam}
import org.apache.griffin.measure.utils.CommonUtils

/**
 * SinkFactory
 *
 * Responsible for creation of Batch and Streaming Sinks based on the definition
 * provided in Env Config.
 */
object SinkFactory extends Loggable with Serializable {

  /**
   *
   * @param sinkParamIter [[Seq]] of sink definitions as [[SinkParam]]
   * @return
   */
  def getSinks(appConfig: AppConfig, sinkParamIter: Seq[SinkParam]): Seq[Sink] = {
    sinkParamIter.map((sinkParam: SinkParam) => getSink(appConfig, sinkParam))
  }

  /**
   * Instantiates a `Sink` (batch or streaming based on user defined configuration).
   *
   * @param sinkParam sink param which holds the user defined configuration
   * @return `Sink` instance
   */
  def getSink(appConfig: AppConfig, sinkParam: SinkParam): Sink = {
    Try {
      val cls = getSinkClass(sinkParam)

      if (sinkParam.getIsStreaming) {
        if (classOf[StreamingSink].isAssignableFrom(cls)) {
          getSinkInstance(cls, appConfig, sinkParam)
        } else {
          val errorMsg =
            s"Data Source with class name '${cls.getCanonicalName}' does not support streaming."
          val exception = new IllegalStateException(errorMsg)
          throw exception
        }
      } else if (classOf[BatchSink].isAssignableFrom(cls)) {
        getSinkInstance(cls, appConfig, sinkParam)
      } else if (classOf[MetricSink].isAssignableFrom(cls)) {
        getSinkInstance(cls, appConfig, sinkParam)
      } else {
        val errorMsg =
          s"Class name '${cls.getCanonicalName}' cannot be instantiated as a valid Metric, Batch or Streaming Sink."
        val exception = new ClassCastException(errorMsg)
        throw exception
      }
    } match {
      case Success(sink) => sink
      case Failure(exception) =>
        error(
          s"""Exception occurred while getting sink with name '${sinkParam.getName}'
             |and type '${sinkParam.getType}'""".stripMargin,
          exception)
        throw exception
    }
  }

  /**
   * Returns a `Sink` class corresponding to the type defined in the configuration.
   * If type does not match any of the internal types (regexes), then its assumed to be class name.
   *
   * [Regex -> Class] mapping of all internally supported `Sink`s must be added as a `case`.
   *
   * Note: These classes must exist on Griffin Class path. To do this users can set
   * `spark.jars` in spark config section of env config.
   * @param sinkParam sink param
   * @return [[Class]] of the `Sink`
   */
  private def getSinkClass(sinkParam: SinkParam): Class[_] = {
    val sinkType = sinkParam.getType

    Try {
      sinkType match {
        case ConsoleRegex() => classOf[ConsoleSink]
        case FileRegex() => classOf[FileSink]
        case HDFSRegex() => classOf[FileSink]
        case className =>
          Class.forName(className, true, CommonUtils.getContextOrMainClassLoader)
      }
    } match {
      case Failure(e: ClassNotFoundException) =>
        error(s"Class for sink of type '$sinkType' was not found on the classpath.", e)
        throw e
      case Failure(exception) =>
        error(s"Unable to initialize sink of type '$sinkType'", exception)
        throw exception
      case Success(cls) => cls
    }
  }

  /**
   * Creates an instance of the data source based on the class.
   *
   * @throws ClassCastException when the provided class name does not extend [[Sink]]
   * @param sinkParam sink param
   * @param cls [[Class]] of the data source
   * @return A new instance of the data source
   */
  private def getSinkInstance(cls: Class[_], appConfig: AppConfig, sinkParam: SinkParam): Sink = {
    debug(s"Attempting to instantiate class with name ${cls.getCanonicalName}")

    Try(
      cls
        .getConstructor(classOf[AppConfig], classOf[SinkParam])
        .newInstance(appConfig, sinkParam)
        .asInstanceOf[Sink]) match {
      case Success(c) =>
        info(s"Successfully instantiated sink with name '${sinkParam.getName}'")
        c
      case Failure(exception) =>
        error(
          s"Error encountered while instantiating sink with name '${sinkParam.getName}'",
          exception)
        throw exception
    }
  }

}
