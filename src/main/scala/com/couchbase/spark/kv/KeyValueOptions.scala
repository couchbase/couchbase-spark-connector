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
package com.couchbase.spark.kv

import com.couchbase.spark.DefaultConstants

/**
 * Helper object to provide type-safe keys and values for Spark SQL query options.
 */
object KeyValueOptions {

  /**
   * Option Key: The name of the bucket, which overrides the implicit bucket configured.
   */
  val Bucket = "bucket"

  /**
   * Option Key: The name of the scope, which overrides the implicit scope (if configured at all).
   *
   * Note: only works against Couchbase Server 7.0 or later.
   */
  val Scope = "scope"

  /**
   * Option Key: The name of the collection, which overrides the implicit collection (if configured at all).
   *
   * Note: only works against Couchbase Server 7.0 or later.
   */
  val Collection = "collection"

  /**
   * Option Key: The field name of the document ID, used to override the default.
   *
   * The default can be located in [[DefaultConstants.DefaultIdFieldName]]
   */
  val IdFieldName = "idFieldName"

  /**
   * Option key: The durability level of write operations, used to override the default.
   *
   * The default is "none", so no durability applied on write operations.
   */
  val Durability = "durability"

  /**
   * Option value: Majority Durability - to be used with [[Durability]] as the key.
   */
  val MajorityDurability = "majority"

  /**
   * Option value: Majority And Persist To Active Durability - to be used with [[Durability]] as the key.
   */
  val MajorityAndPersistToActiveDurability = "majorityAndPersistToActive"

  /**
   * Option value: Persist To Majority Durability - to be used with [[Durability]] as the key.
   */
  val PersistToMajorityDurability = "PersistToMajority"

}
