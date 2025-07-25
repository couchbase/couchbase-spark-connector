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
package com.couchbase.spark.enterpriseanalytics

import com.couchbase.spark.DefaultConstants

/** Helper object to provide type-safe keys and values for Spark SQL Columnar query options.
  */
object EnterpriseAnalyticsOptions {

  /** Mandatory 'option': specifies the Enterprise Analytics database to use. */
  val Database   = "database"

  /** Mandatory 'option': specifies the Enterprise Analytics scope to use. */
  val Scope      = "scope"

  /** Mandatory 'option': specifies the Enterprise Analytics collection to use. */
  val Collection = "collection"

  /** Option Key: a SQL++ expression which acts as an additional filter on every columnar query,
    * including schema inference.
    */
  val Filter = "filter"

  /** Option Key: Allows to override the scan consistency for the query performed.
    */
  val ScanConsistency = "scanConsistency"

  /** Option Key: The limit of how many records to load during schema inference.
    *
    * The default can be found in [[DefaultConstants.DefaultInferLimit]]
    */
  val InferLimit = "inferLimit"

  /** Option Key: The timeout to use which overrides the default configured.
    *
    * The value is a string and must be parsable from a Scala Duration.
    */
  val Timeout = "timeout"

  /** Option Value: Not bounded scan consistency - to be used with [[ScanConsistency]] as the key.
    *
    * This is the default and usually does not need to be specified. The query will be executed
    * immediately, and it might not include documents which have just been written and not made it
    * into the index yet.
    */
  val NotBoundedScanConsistency = "notBounded"

  /** Option Value: Request plus scan consistency - to be used with [[ScanConsistency]] as the key.
    *
    * If this value is set, the query will wait until it has caught up to the recent mutations,
    * which will make sure that documents which just have been written are part of the result set.
    */
  val RequestPlusScanConsistency = "requestPlus"

}
