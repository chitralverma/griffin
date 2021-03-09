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

import scala.collection.mutable
import scala.util._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQuery

import org.apache.griffin.measure.Loggable
import org.apache.griffin.measure.utils.SparkSessionFactory

object StreamingQueryRegister extends Loggable with Serializable {

  private val sparkSession: SparkSession = SparkSessionFactory.getInstance
  private val queries: mutable.Set[StreamingQuery] = mutable.HashSet.empty

  def registerQuery[T](query: StreamingQuery): Unit = {
    debug(s"Registering streaming query with name '${query.name}'")
    queries.add(query)
  }

  def existsQuery(name: String): Boolean = {
    debug(s"Checking if streaming query with name '$name' exists")
    queries.exists(q => name.equalsIgnoreCase(q.name))
  }

  def getQueries: Set[StreamingQuery] = {
    debug(s"Getting all streaming queries")
    queries.toSet
  }

  def getQuery(name: String): Option[StreamingQuery] = {
    debug(s"Getting streaming query with name '$name', if it exists.")
    queries.find(q => name.equalsIgnoreCase(q.name))
  }

  private def stopQuery(query: StreamingQuery): Boolean = stopQuery(query.name)

  private def stopQuery(name: String): Boolean = {
    debug(s"Stopping streaming query with name '$name'")

    getQuery(name) match {
      case Some(query) =>
        Try {
          query.stop()
          sparkSession.streams.resetTerminated()
          sparkSession.streams.awaitAnyTermination()
        } match {
          case Success(_) =>
            debug(
              s"Successfully stopped streaming query with name '$name'. Spark awaitAnyTermination is not reset.")
            queries.remove(query)
            true
          case Failure(exception) =>
            error(
              s"Exception occurred while stopping streaming query with name '$name'.",
              exception)
            false
        }

      case None =>
        warn(s"Streaming query with name '$name' was not found.")
        false
    }
  }

  def stopAllQueries(): Boolean = {
    debug(s"Attempting to gracefully stop all streaming queries.")

    val res = getQueries.forall(stopQuery)

    if (queries.nonEmpty) {
      warn(s"Query set was not empty.")
      queries.clear()
    }

    res
  }

}
