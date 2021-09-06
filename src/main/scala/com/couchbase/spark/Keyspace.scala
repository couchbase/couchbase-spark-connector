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

/**
 * The keyspace reflects a triple/coordinate of bucket, scope and collection.
 *
 * Note that not all APIs need all three values to be set. Depending on the context where the keyspace is used or
 * the type of service (i.e. kv vs. query) it might be sufficient to only provide a subset. See the individual semantics
 * for each operation if in doubt.
 *
 * @param bucket the bucket name, if present.
 * @param scope the scope name, if present.
 * @param collection the collection name, if present.
 */
case class Keyspace(bucket: Option[String] = None, scope: Option[String] = None, collection: Option[String] = None)
