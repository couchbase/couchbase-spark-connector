/*
 * Copyright (c) 2026 Couchbase, Inc.
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

import org.junit.jupiter.api.Assertions.{assertEquals, assertNotEquals, assertTrue}
import org.junit.jupiter.api.Test

import java.nio.charset.StandardCharsets

class KeyValueErrorDocumentHandlerTest {
  private def id(bucket: String, scope: String, collection: String, documentId: String, ts: Long = 1000L) =
    KeyValueErrorDocumentHandler.errorDocumentId(bucket, scope, collection, documentId, ts)

  @Test
  def format(): Unit = {
    assertTrue(id("b", "s", "c", "doc1").matches("error_[0-9a-f]{32}_1000"))
  }

  @Test
  def maxLengthDocumentIdStaysWithinKeyLimit(): Unit = {
    // A 250-byte original ID, which would previously have produced an invalid error document key.
    val errorId = id("a-bucket-name", "a-scope", "a-collection", "x" * 250)
    assertTrue(errorId.getBytes(StandardCharsets.UTF_8).length < 250)
  }

  @Test
  def multiByteDocumentId(): Unit = {
    assertTrue(id("b", "s", "c", "ü" * 125).matches("error_[0-9a-f]{32}_1000"))
  }

  @Test
  def sameDocumentSameHash(): Unit = {
    val first  = id("b", "s", "c", "doc1", 1000L)
    val second = id("b", "s", "c", "doc1", 2000L)
    assertEquals(first.stripSuffix("_1000"), second.stripSuffix("_2000"))
    assertNotEquals(first, second)
  }

  @Test
  def locationIsPartOfTheHash(): Unit = {
    assertNotEquals(id("b", "s", "c", "doc1"), id("b", "s", "c2", "doc1"))
    assertNotEquals(id("b", "s", "c", "doc1"), id("b2", "s", "c", "doc1"))
    // Bucket names can contain '.', which the old dotted format could not distinguish.
    assertNotEquals(id("b.s", "c", "d", "doc1"), id("b", "s.c", "d", "doc1"))
  }
}
