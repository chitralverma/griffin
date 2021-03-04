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

package org.apache.griffin.measure

import scala.util.{Failure, Success}

import org.apache.griffin.measure.configuration.dqdefinition.{AppConfig, EnvConfig, GriffinConfig}
import org.apache.griffin.measure.configuration.dqdefinition.reader.ParamReaderFactory.readParam
import org.apache.griffin.measure.execution.GriffinJobExecutor
import org.apache.griffin.measure.utils.SparkSessionFactory

/**
 * Apache Griffin Application
 *
 * This acts as the starting point of a Data Quality Job.
 */
object Application extends Loggable {

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      error("Usage: class <env-param> <dq-param>")
      sys.exit(-1)
    }

    // Initialize Griffin Configurations
    val envConfig = readParam[EnvConfig](args(0))
    val appConfig = readParam[AppConfig](args(1))
    val griffinConfig: GriffinConfig = GriffinConfig(envConfig, appConfig)

    // Execute Griffin Job
    val jobStatus = GriffinJobExecutor(griffinConfig).execute()

    jobStatus match {
      case Success(result) => info("process run result: " + (if (result) "success" else "failed"))
      case Failure(exception) =>
        error(s"Fatal Exception occurred!", exception)
        throw exception
    }

    SparkSessionFactory.close()
  }

}
