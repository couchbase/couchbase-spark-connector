/*
 * Copyright (c) 2015 Couchbase, Inc.
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
package com.couchbase.spark.streaming

sealed class StreamFrom
case object FromBeginning extends StreamFrom
 case object FromNow extends StreamFrom
// final case class FromSequence(seqno: Int) extends StreamFrom


sealed class StreamTo
case object ToInfinity /* and beyond! */ extends StreamTo
// final case class ToSequence(seqno: Int) extends StreamTo