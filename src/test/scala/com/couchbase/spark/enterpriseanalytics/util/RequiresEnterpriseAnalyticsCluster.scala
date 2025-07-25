/*
 * Copyright (c) 2025 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.couchbase.spark.enterpriseanalytics.util

import com.couchbase.spark.util.TestOptionsPropertyLoader
import org.junit.jupiter.api.extension.{ConditionEvaluationResult, ExecutionCondition, ExtensionContext}

class RequiresEnterpriseAnalyticsCluster extends ExecutionCondition {
  private val testOptionsPropertyLoader = new TestOptionsPropertyLoader()

  override def evaluateExecutionCondition(context: ExtensionContext): ConditionEvaluationResult = {
    val clusterType = testOptionsPropertyLoader.clusterType
    if ("enterpriseanalytics" == clusterType) {
      ConditionEvaluationResult.enabled("Test is enabled for 'enterpriseanalytics' cluster type")
    } else {
      ConditionEvaluationResult.disabled("Test is disabled for non-'enterpriseanalytics' cluster type")
    }
  }
}