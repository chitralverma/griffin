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

import java.sql.DriverManager
import java.util.Properties

import scala.util._

import org.apache.griffin.measure.configuration.dqdefinition.DataSourceParam
import org.apache.griffin.measure.SparkSuiteBase

class JDBCDataSourceTest extends SparkSuiteBase {

  val url = "jdbc:h2:mem:test"
  var conn: java.sql.Connection = _
  val properties = new Properties()
  properties.setProperty("user", "user")
  properties.setProperty("password", "password")
  properties.setProperty("rowId", "false")

  override def beforeAll(): Unit = {
    super.beforeAll()
    DriverManager.registerDriver(new org.h2.Driver)
    Class.forName("org.h2.Driver", false, this.getClass.getClassLoader)
    conn = DriverManager.getConnection(url, properties)
    conn.prepareStatement("create schema griffin").executeUpdate()

    conn.prepareStatement("drop table if exists griffin.employee").executeUpdate()
    conn
      .prepareStatement(
        "create table griffin.employee (name VARCHAR NOT NULL, id INTEGER NOT NULL)")
      .executeUpdate()
    conn.prepareStatement("insert into griffin.employee values ('emp1', 1)").executeUpdate()
    conn.prepareStatement("insert into griffin.employee values ('emp2', 2)").executeUpdate()
    conn.commit()
  }

  private final val dcParam = DataSourceParam(
    name = "datasource",
    conType = "jdbc",
    dataFrameName = "test_df",
    config = Map.empty,
    preProc = Nil)

  "JDBC based data source" should "be able to read data from relational database" in {
    val configs = Map(
      "database" -> "griffin",
      "tablename" -> "employee",
      "url" -> url,
      "user" -> "user",
      "password" -> "password",
      "driver" -> "org.h2.Driver")
    val dc = new JDBCDataSource(dcParam.copy(config = configs))
    val result = Try(dc.readBatch()).toOption
    assert(result.isDefined)
    assert(result.get.collect().length == 2)
  }

  "JDBC based data source" should "be able to read data from relational database with where condition" in {
    val configs = Map(
      "database" -> "griffin",
      "tablename" -> "employee",
      "url" -> url,
      "user" -> "user",
      "password" -> "password",
      "driver" -> "org.h2.Driver",
      "where" -> "id=1 and name='emp1'")
    val dc = new JDBCDataSource(dcParam.copy(config = configs))
    val result = Try(dc.readBatch()).toOption
    assert(result.isDefined)
    assert(result.get.collect().length == 1)
  }

  "JDBC data source" should "have URL field in config" in {
    the[java.lang.IllegalArgumentException] thrownBy {
      val configs = Map(
        "database" -> "griffin",
        "tablename" -> "employee",
        "user" -> "user",
        "password" -> "password",
        "driver" -> "org.h2.Driver")
      new JDBCDataSource(dcParam.copy(config = configs)).validate()
    } should have message "requirement failed: JDBC connection: connection url is mandatory"
  }

  "JDBC data source" should "have table name field in config" in {
    the[java.lang.IllegalArgumentException] thrownBy {
      val configs = Map(
        "database" -> "griffin",
        "url" -> url,
        "user" -> "user",
        "password" -> "password",
        "driver" -> "org.h2.Driver")
      new JDBCDataSource(dcParam.copy(config = configs)).validate()
    } should have message "requirement failed: JDBC connection: table is mandatory"
  }

  "JDBC data source" should "have user name field in config" in {
    the[java.lang.IllegalArgumentException] thrownBy {
      val configs = Map(
        "database" -> "griffin",
        "url" -> url,
        "tablename" -> "employee",
        "password" -> "password",
        "driver" -> "org.h2.Driver")
      new JDBCDataSource(dcParam.copy(config = configs)).validate()
    } should have message "requirement failed: JDBC connection: user name is mandatory"
  }

  "JDBC data source" should "have table password field in config" in {
    the[java.lang.IllegalArgumentException] thrownBy {
      val configs = Map(
        "database" -> "griffin",
        "url" -> url,
        "tablename" -> "employee",
        "user" -> "user",
        "driver" -> "org.h2.Driver")
      new JDBCDataSource(dcParam.copy(config = configs)).validate()
    } should have message "requirement failed: JDBC connection: password is mandatory"
  }

  "JDBC data source" should "have driver provided in config in classpath" in {
    the[AssertionError] thrownBy {
      val configs = Map(
        "database" -> "griffin",
        "url" -> url,
        "tablename" -> "employee",
        "user" -> "user",
        "password" -> "password",
        "driver" -> "org.postgresql.Driver")
      new JDBCDataSource(dcParam.copy(config = configs)).validate()
    }
  }

  override def afterAll(): Unit = {
    super.afterAll()
    conn.close()
  }
}
