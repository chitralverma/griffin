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

package org.apache.griffin.measure.configuration.dqdefinition

import com.fasterxml.jackson.annotation.{JsonInclude, JsonProperty}
import com.fasterxml.jackson.annotation.JsonInclude.Include
import org.apache.commons.lang.StringUtils

import org.apache.griffin.measure.configuration.enums._

/**
 * dq param
 * @param name           name of dq measurement (must)
 * @param dataSources    data sources (must)
 * @param evaluateRule   dq measurement (must)
 * @param sinks          sink types (optional, by default will be elasticsearch)
 */
@JsonInclude(Include.NON_NULL)
case class AppConfig(
    @JsonProperty("name") name: String,
    @JsonProperty("data.sources") dataSources: List[DataSourceParam],
    @JsonProperty("evaluate.rule") evaluateRule: EvaluateRuleParam,
    @JsonProperty("sinks") sinks: List[String] = Nil)
    extends Param {
  def getName: String = name
  def getDataSourceParams: Seq[DataSourceParam] = {
    dataSources
      .foldLeft((Nil: Seq[DataSourceParam], Set[String]())) { (ret, ds) =>
        val (seq, names) = ret
        if (!names.contains(ds.getName)) {
          (seq :+ ds, names + ds.getName)
        } else ret
      }
      ._1
  }
  def getEvaluateRule: EvaluateRuleParam = evaluateRule
  def getSinkNames: Seq[String] = sinks
  def getValidSinkTypes: Seq[SinkType.SinkType] = SinkType.validSinkTypes(sinks)

  def validate(): Unit = {
    assert(StringUtils.isNotBlank(name), "dq config name should not be blank")
    assert(dataSources != null, "data.sources should not be null")
    assert(evaluateRule != null, "evaluate.rule should not be null")
    getDataSourceParams.foreach(_.validate())
    evaluateRule.validate()
  }
}

/**
 * data source param
 * @param name data source name (must)
 * @param conType data source type, e.g.: hive, avro, kafka (must)
 * @param isStreaming is this a streaming data source or a batch data source
 * @param config detail configuration of data source (must)
 * @param preProc pre-process rules after load data (optional)
 * @param dataFrameName data source dataframe name, for pre-process input usage (optional)
 */
@JsonInclude(Include.NON_NULL)
case class DataSourceParam(
    @JsonProperty("name") private val name: String,
    @JsonProperty("type") private val conType: String,
    @JsonProperty("isStreaming") private val isStreaming: Boolean = false,
    @JsonProperty("config") private val config: Map[String, Any],
    @JsonProperty("pre.proc") private val preProc: List[RuleParam],
    @JsonProperty("dataframe.name") private val dataFrameName: String)
    extends Param {
  def getName: String = name
  def getType: String = conType
  def getIsStreaming: Boolean = isStreaming
  def getConfig: Map[String, Any] = if (config != null) config else Map[String, Any]()
  def getPreProcRules: Seq[RuleParam] = if (preProc != null) preProc else Nil
  def getDataFrameName(defName: String): String =
    if (dataFrameName != null) dataFrameName else defName

  def validate(): Unit = {
    assert(StringUtils.isNotBlank(name), "data source name should not be empty")
    assert(conType != null, "data source type should not be empty")
    assert(StringUtils.isNotBlank(conType), "data source type should not be empty")
    getPreProcRules.foreach(_.validate())
  }
}

/**
 * evaluate rule param
 * @param rules      rules to define dq measurement (optional)
 */
@JsonInclude(Include.NON_NULL)
case class EvaluateRuleParam(@JsonProperty("rules") private val rules: List[RuleParam])
    extends Param {
  def getRules: Seq[RuleParam] = if (rules != null) rules else Nil

  def validate(): Unit = {
    getRules.foreach(_.validate())
  }
}

/**
 * rule param
 * @param dslType    dsl type of this rule (must)
 * @param dqType     dq type of this rule (must if dsl type is "griffin-dsl")
 * @param inDfName   name of input dataframe of this rule, by default will be the previous rule output dataframe name
 * @param outDfName  name of output dataframe of this rule, by default will be generated
 *                   as dataframe name with index suffix
 * @param rule       rule to define dq step calculation (must)
 * @param details    detail config of rule (optional)
 * @param cache      cache the result for multiple usage (optional, valid for "spark-sql" and "df-ops" mode)
 * @param outputs    output ways configuration (optional)
 * @param errorConfs error configuration (valid for 'COMPLETENESS' mode)
 */
@JsonInclude(Include.NON_NULL)
case class RuleParam(
    @JsonProperty("dsl.type") private val dslType: String,
    @JsonProperty("dq.type") private val dqType: String,
    @JsonProperty("in.dataframe.name") private val inDfName: String = null,
    @JsonProperty("out.dataframe.name") private val outDfName: String = null,
    @JsonProperty("rule") private val rule: String = null,
    @JsonProperty("details") private val details: Map[String, Any] = null,
    @JsonProperty("cache") private val cache: Boolean = false,
    @JsonProperty("out") private val outputs: List[RuleOutputParam] = null,
    @JsonProperty("error.confs") private val errorConfs: List[RuleErrorConfParam] = null)
    extends Param {
  def getDslType: DslType.DslType =
    if (dslType != null) DslType.withNameWithDefault(dslType) else DslType.GriffinDsl
  def getDqType: DqType.DqType =
    if (dqType != null) DqType.withNameWithDefault(dqType) else DqType.Unknown
  def getCache: Boolean = if (cache) cache else false

  def getInDfName(defName: String = ""): String = if (inDfName != null) inDfName else defName
  def getOutDfName(defName: String = ""): String = if (outDfName != null) outDfName else defName
  def getRule: String = if (rule != null) rule else ""
  def getDetails: Map[String, Any] = if (details != null) details else Map[String, Any]()

  def getOutputs: Seq[RuleOutputParam] = if (outputs != null) outputs else Nil
  def getOutputOpt(tp: OutputType.OutputType): Option[RuleOutputParam] =
    getOutputs.find(_.getOutputType == tp)

  def getErrorConfs: Seq[RuleErrorConfParam] = if (errorConfs != null) errorConfs else Nil

  def replaceInDfName(newName: String): RuleParam = {
    if (StringUtils.equals(newName, inDfName)) this
    else RuleParam(dslType, dqType, newName, outDfName, rule, details, cache, outputs)
  }
  def replaceOutDfName(newName: String): RuleParam = {
    if (StringUtils.equals(newName, outDfName)) this
    else RuleParam(dslType, dqType, inDfName, newName, rule, details, cache, outputs)
  }
  def replaceInOutDfName(in: String, out: String): RuleParam = {
    if (StringUtils.equals(inDfName, in) && StringUtils.equals(outDfName, out)) this
    else RuleParam(dslType, dqType, in, out, rule, details, cache, outputs)
  }
  def replaceRule(newRule: String): RuleParam = {
    if (StringUtils.equals(newRule, rule)) this
    else RuleParam(dslType, dqType, inDfName, outDfName, newRule, details, cache, outputs)
  }

  def validate(): Unit = {
    assert(
      !(getDslType.equals(DslType.GriffinDsl) && getDqType.equals(DqType.Unknown)),
      "unknown dq type for griffin dsl")

    getOutputs.foreach(_.validate())
    getErrorConfs.foreach(_.validate())
  }
}

/**
 * out param of rule
 * @param outputType     output type (must)
 * @param name           output name (optional)
 * @param flatten        flatten type of output metric (optional, available in output metric type)
 */
@JsonInclude(Include.NON_NULL)
case class RuleOutputParam(
    @JsonProperty("type") private val outputType: String,
    @JsonProperty("name") private val name: String,
    @JsonProperty("flatten") private val flatten: String)
    extends Param {
  def getOutputType: OutputType.OutputType = {
    if (outputType != null) OutputType.withNameWithDefault(outputType)
    else OutputType.UnknownOutputType
  }
  def getNameOpt: Option[String] = Some(name).filter(StringUtils.isNotBlank)
  def getFlatten: FlattenType.FlattenType = {
    if (StringUtils.isNotBlank(flatten)) FlattenType.withNameWithDefault(flatten)
    else FlattenType.DefaultFlattenType
  }

  def validate(): Unit = {}
}

/**
 * error configuration parameter
 * @param columnName the name of the column
 * @param errorType  the way to match error, regex or enumeration
 * @param values     error value list
 */
@JsonInclude(Include.NON_NULL)
case class RuleErrorConfParam(
    @JsonProperty("column.name") private val columnName: String,
    @JsonProperty("type") private val errorType: String,
    @JsonProperty("values") private val values: List[String])
    extends Param {
  def getColumnName: Option[String] = Some(columnName).filter(StringUtils.isNotBlank)
  def getErrorType: Option[String] = Some(errorType).filter(StringUtils.isNotBlank)
  def getValues: Seq[String] = if (values != null) values else Nil

  def validate(): Unit = {
    assert(
      "regex".equalsIgnoreCase(getErrorType.get) ||
        "enumeration".equalsIgnoreCase(getErrorType.get),
      "error error.conf type")
  }
}
