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

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import org.apache.griffin.measure.utils.SparkSessionFactory

trait SparkSuiteBase extends GriffinTestBase {

  var spark: SparkSession = _

  override def beforeAll() {
    super.beforeAll()
    cleanTestHiveData()
    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("GriffinSparkSuiteBase")
      .set("spark.sql.crossJoin.enabled", "true")

    spark = SparkSessionFactory.create(conf.getAll.toMap)
  }

  override def afterAll() {
    try {
      SparkSessionFactory.close()
      cleanTestHiveData()
    } finally {
      super.afterAll()
    }
  }

  def cleanTestHiveData(): Unit = {
    val metastoreDB = new File("metastore_db")
    if (metastoreDB.exists) {
      FileUtils.forceDelete(metastoreDB)
    }
    val sparkWarehouse = new File("spark-warehouse")
    if (sparkWarehouse.exists) {
      FileUtils.forceDelete(sparkWarehouse)
    }
  }
}
