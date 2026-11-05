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

private[spark] object WhereClause {

  /** Combines conditions into a SQL++ WHERE clause.
    *
    * Each condition is parenthesised before being ANDed, so a condition containing a top-level OR
    * (most likely a user-supplied filter) cannot bind with its neighbours. Without this,
    * `a OR b` followed by `AND c` would be evaluated as `a OR (b AND c)`.
    *
    * @param conditions
    *   the conditions to combine. Absent and blank conditions are skipped.
    * @return
    *   `" WHERE (c1) AND (c2) ..."` with a leading space, or `""` if there are no conditions.
    */
  def build(conditions: Option[String]*): String = {
    val present = conditions.flatten.map(_.trim).filter(_.nonEmpty)
    if (present.isEmpty) ""
    else present.map(c => s"($c)").mkString(" WHERE ", " AND ", "")
  }
}
