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

import org.apache.spark.sql.{Dataset, Row}

import org.apache.griffin.measure.configuration.dqdefinition.DataSourceParam
import org.apache.griffin.measure.datasource.BatchDataSource
import org.apache.griffin.measure.utils.ParamUtil._

class HiveDataSource(dataSourceParam: DataSourceParam) extends BatchDataSource(dataSourceParam) {
  override type T = Row
  override type Connector = Unit

  val config: Map[String, Any] = dataSourceParam.getConfig

  val Database = "database"
  val TableName = "table.name"
  val Where = "where"

  val database: String = config.getString(Database, "default")
  val tableName: String = config.getString(TableName, "")
  val whereClause: String = config.getString(Where, "true")

  val concreteTableName = s"$database.$tableName"
  val wheres: Array[String] = whereClause.split(",").map(_.trim).filter(_.nonEmpty)

  override def validate(): Unit = {
    assert(whereClause != null || whereClause.nonEmpty, "Invalid where clause.")
  }

  override protected def initializeConnector(): Unit = {}

  override def readBatch(): Dataset[Row] = {
    sparkSession.read.table(concreteTableName).where(whereClause)
  }

}
