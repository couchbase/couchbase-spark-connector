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

import com.couchbase.client.scala.kv.{LookupInSpec, MutateInSpec}

sealed abstract class KeyValueOperation

case class Get(id: String)                                               extends KeyValueOperation
case class Insert[T](id: String, content: T)                             extends KeyValueOperation
case class Upsert[T](id: String, content: T)                             extends KeyValueOperation
case class Replace[T](id: String, content: T, cas: Long = 0)             extends KeyValueOperation
case class Remove(id: String, cas: Long = 0)                             extends KeyValueOperation
case class MutateIn(id: String, specs: Seq[MutateInSpec], cas: Long = 0) extends KeyValueOperation
case class LookupIn(id: String, specs: Seq[LookupInSpec])                extends KeyValueOperation
