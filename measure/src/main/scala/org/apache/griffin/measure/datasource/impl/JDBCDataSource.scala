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

import java.util.Properties

import org.apache.spark.sql.{Dataset, Row}

import org.apache.griffin.measure.configuration.dqdefinition.DataSourceParam
import org.apache.griffin.measure.datasource.BatchDataSource
import org.apache.griffin.measure.utils.ParamUtil._

/**
 * A batch data source for JDBC based source which allows support for various
 * JDBC based data sources like Oracle. Postgres etc.
 *
 * Supported Configurations:
 *  - database : [[String]] specifying the database name.
 *  - tablename : [[String]] specifying the table name to be read
 *  - url : [[String]] specifying the URL to connect to database
 *  - user : [[String]] specifying the user for connection to database
 *  - password: [[String]] specifying the password for connection to database
 *  - driver : [[String]] specifying the driver for JDBC connection to database
 *  - where : [[String]] specifying the condition for reading data from table
 *
 * Some defaults assumed by this source (if not set) are as follows:
 *  - `database` is default,
 *  - `driver` is com.mysql.jdbc.Driver,
 *  - `where` is None
 */
class JDBCDataSource(val dataSourceParam: DataSourceParam) extends BatchDataSource {
  override type T = Row
  override type Connector = Unit

  import JDBCDataSource._

  val config: Map[String, Any] = dataSourceParam.getConfig
  val database: String = config.getString(Database, DefaultDatabase)
  val tableName: String = config.getString(TableName, EmptyString)
  val fullTableName: String = s"$database.$tableName"
  val whereString: String = config.getString(Where, EmptyString)
  val url: String = config.getString(Url, EmptyString)
  val user: String = config.getString(User, EmptyString)
  val password: String = config.getString(Password, EmptyString)
  val driver: String = config.getString(Driver, DefaultDriver)

  /**
   * @param driver JDBC driver class name
   * @return True if JDBC driver present in classpath
   */
  private def isJDBCDriverLoaded(driver: String): Boolean = {
    try {
      Class.forName(driver, false, this.getClass.getClassLoader)
      true
    } catch {
      case e: ClassNotFoundException =>
        error(s"JDBC driver $driver provided is not found in class path", e)
        false
    }
  }

  override def validate(): Unit = {
    require(url.nonEmpty, "JDBC connection: connection url is mandatory")
    require(user.nonEmpty, "JDBC connection: user name is mandatory")
    require(password.nonEmpty, "JDBC connection: password is mandatory")
    require(tableName.nonEmpty, "JDBC connection: table is mandatory")
    assert(isJDBCDriverLoaded(driver), s"JDBC driver $driver not present in classpath")
  }

  override protected def initializeConnector(): Unit = {}

  override def readBatch(): Dataset[Row] = {
    val dtSql = createSqlStmt()
    val prop = new Properties()
    prop.setProperty("user", user)
    prop.setProperty("password", password)
    prop.setProperty("driver", driver)

    sparkSession.read.jdbc(url, s"($dtSql) as t", prop)
  }

  /**
   * @return Return SQL statement with where condition if provided
   */
  private def createSqlStmt(): String = {
    val tableClause = s"SELECT * FROM $fullTableName"
    if (whereString.nonEmpty) {
      s"$tableClause WHERE $whereString"
    } else tableClause
  }
}

object JDBCDataSource {
  private val Database: String = "database"
  private val TableName: String = "tablename"
  private val Where: String = "where"
  private val Url: String = "url"
  private val User: String = "user"
  private val Password: String = "password"
  private val Driver: String = "driver"

  private val DefaultDriver = "com.mysql.jdbc.Driver"
  private val DefaultDatabase = "default"
  private val EmptyString = ""
}
