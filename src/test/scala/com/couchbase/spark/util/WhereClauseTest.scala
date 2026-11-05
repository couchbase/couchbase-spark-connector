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
package com.couchbase.spark.util

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class WhereClauseTest {

  @Test
  def noConditions(): Unit = {
    assertEquals("", WhereClause.build())
    assertEquals("", WhereClause.build(None, None))
  }

  @Test
  def blankConditionsAreSkipped(): Unit = {
    assertEquals("", WhereClause.build(Some(""), Some("   ")))
    assertEquals(" WHERE (a = 1)", WhereClause.build(Some(""), Some("a = 1"), None))
  }

  @Test
  def singleCondition(): Unit = {
    assertEquals(" WHERE (a = 1)", WhereClause.build(Some("a = 1")))
  }

  @Test
  def conditionsAreTrimmed(): Unit = {
    // QueryFilters.compile output has a leading space
    assertEquals(" WHERE (a = 1) AND (`b` > 2)", WhereClause.build(Some("a = 1 "), Some(" `b` > 2")))
  }

  @Test
  def userFilterWithOrIsIsolatedFromPushedFilters(): Unit = {
    assertEquals(
      " WHERE (a = 1 OR b = 2) AND (`c` IS NOT NULL AND `c` > 3)",
      WhereClause.build(Some("a = 1 OR b = 2"), Some(" `c` IS NOT NULL AND `c` > 3"))
    )
  }

  @Test
  def userFilterWithOrIsIsolatedFromPartitionBound(): Unit = {
    assertEquals(
      " WHERE (a = 1 OR b = 2) AND (id >= 5001 AND id < 10001)",
      WhereClause.build(Some("a = 1 OR b = 2"), Some(""), Some("id >= 5001 AND id < 10001"))
    )
  }

  @Test
  def allThreeConditions(): Unit = {
    assertEquals(
      " WHERE (a = 1 OR b = 2) AND (`c` > 3) AND (id < 5001)",
      WhereClause.build(Some("a = 1 OR b = 2"), Some(" `c` > 3"), Some("id < 5001"))
    )
  }
}
