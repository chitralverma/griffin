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

package org.apache.griffin.measure.step.write

import scala.util.Try

import org.apache.griffin.measure.context.DQContext
import org.apache.griffin.measure.execution.StreamingQueryRegister
import org.apache.griffin.measure.sink.{BatchSink, StreamingSink}

/**
 * write records needs to be sink
 */
case class RecordWriteStep(
    name: String,
    inputName: String,
    filterTableNameOpt: Option[String] = None,
    writeTimestampOpt: Option[Long] = None)
    extends WriteStep {

  def execute(context: DQContext): Try[Boolean] = Try {
    val records = context.sparkSession.table(s"`$inputName`")

    // write records
    val res = context.getSinks.map {
      case sink: BatchSink =>
        sink.sinkBatchRecords(name, records)
        true
      case sink: StreamingSink =>
        val streamingQuery = sink.sinkStreamingRecords(records)
        StreamingQueryRegister.registerQuery(streamingQuery)
        true
      case sink =>
        val errorMsg =
          s"Sink with name '${sink.sinkParam.getName}' does not support record writing."
        warn(errorMsg)
        true
    }

    res.forall(x => x)
  }
}
