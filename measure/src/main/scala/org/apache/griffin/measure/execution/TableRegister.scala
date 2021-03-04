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

import scala.collection.mutable.{Set => MutableSet}

import org.apache.spark.sql.Dataset

import org.apache.griffin.measure.Loggable
import org.apache.griffin.measure.utils.SparkSessionFactory

object TableRegister extends Loggable with Serializable {

  private val tables: MutableSet[String] = MutableSet()

  def registerTable[T](name: String, df: Dataset[T]): Unit = {
    griffinLogger.info(s"Registering data set with name '$name'")
    tables.add(name)
    df.createOrReplaceTempView(name)
  }

  def existsTable(name: String): Boolean = {
    griffinLogger.info(s"Checking if data set with name '$name' exists")
    tables.exists(_.equals(name))
  }

  def getTables: Set[String] = {
    tables.toSet
  }

  def unregisterTable(name: String): Unit = {
    griffinLogger.info(s"Un registering data set with name '$name'")

    if (existsTable(name)) {
      SparkSessionFactory.getInstance.catalog.dropTempView(name)
      tables.remove(name)
    }
  }

  def unregisterAllTables(): Unit = {
    griffinLogger.info(s"Un registering all data sets")

    val uts = getTables
    uts.foreach(t => SparkSessionFactory.getInstance.catalog.dropTempView(t))
    tables.clear
  }

}
