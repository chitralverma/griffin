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

package org.apache.griffin.measure.utils

import scala.util.Try

object JDBCUtils {
  object JDBCConstants {
    final val Database: String = "database"
    final val TableName: String = "tablename"
    final val Where: String = "where"
    final val Url: String = "url"
    final val User: String = "user"
    final val Password: String = "password"
    final val Driver: String = "driver"

    final val DefaultDriver = "com.mysql.jdbc.Driver"
    final val DefaultDatabase = "default"
    final val EmptyString = ""
  }

  /**
   * @param driver JDBC driver class name
   * @return True if JDBC driver present in classpath
   */
  def isJDBCDriverLoaded(driver: String): Try[Class[_]] =
    Try(Class.forName(driver, false, this.getClass.getClassLoader))

}
