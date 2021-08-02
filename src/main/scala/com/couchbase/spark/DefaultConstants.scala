/*
 * Copyright (c) 2021 Couchbase, Inc.
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
package com.couchbase.spark

import com.couchbase.client.core.io.CollectionIdentifier

object DefaultConstants {

  /**
   * The default field name used for the document ID when used in queries.
   */
  val DefaultIdFieldName = "__META_ID"

  /**
   * The default query scan consistency to use if not provided by the user.
   */
  val DefaultQueryScanConsistency = "notBounded"

  /**
   * The default analytics scan consistency to use if not provided by the user.
   */
  val DefaultAnalyticsScanConsistency = "notBounded"

  /**
   * The default number of records applied as a limit when infering the schema.
   */
  val DefaultInferLimit = "1000"

  /**
   * The default scope name, if not provided.
   */
  val DefaultScopeName: String = CollectionIdentifier.DEFAULT_SCOPE

  /**
   * The default collection name, if not provided.
   */
  val DefaultCollectionName: String = CollectionIdentifier.DEFAULT_COLLECTION

}
