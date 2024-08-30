/*
 * Copyright (c) 2024 Couchbase, Inc.
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

import java.util.UUID

object TestNameUtil {
  /** Guesses the name of the current test file or test, from the stacktrace. */
  def testName: String = {
    val st = Thread.currentThread.getStackTrace
    st.map(_.getClassName.replace("com.couchbase.spark.", ""))
      .filterNot(_.endsWith("SparkTest"))
      .find(_.endsWith("Test"))
      .getOrElse(st.map(_.getMethodName)
        .find(v => v.endsWith("Test") || v.endsWith("Test$1"))
        .getOrElse(UUID.randomUUID.toString.substring(0, 6)))
      .split("\\.")
      .last
  }
}
