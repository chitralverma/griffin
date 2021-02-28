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

package org.apache.griffin.measure.configuration.dqdefinition.reader

import scala.reflect.ClassTag
import scala.util._

import org.apache.griffin.measure.configuration.dqdefinition.Param
import org.apache.griffin.measure.utils.JsonUtil
import org.apache.griffin.measure.Loggable

object ParamReaderFactory extends Loggable {

  val json = "json"
  val file = "file"
  val httpRegex = "^http[s]?://.*"

  /**
   * Parse given content to get a [[ParamReader]]
   * @param pathOrJson this defines whether the input param definition was provided as a location, string or a path
   * @return instance of a [[ParamReader]]
   */
  def getParamReader(pathOrJson: String): ParamReader = {
    if (pathOrJson.matches(httpRegex)) {
      ParamHttpReader(pathOrJson)
    } else {
      val strType = paramStrType(pathOrJson)
      if (json.equals(strType)) ParamJsonReader(pathOrJson)
      else ParamFileReader(pathOrJson)
    }
  }

  private def paramStrType(str: String): String = {
    try {
      JsonUtil.toAnyMap(str)
      json
    } catch {
      case _: Throwable => file
    }
  }

  def readParam[T <: Param](file: String)(implicit m: ClassTag[T]): T = {
    debug(file)

    val paramReader = getParamReader(file)
    val param = paramReader.readConfig[T]

    param match {
      case Success(p) => p
      case Failure(ex) =>
        error(ex.getMessage, ex)
        sys.exit(-2)
    }
  }

}
