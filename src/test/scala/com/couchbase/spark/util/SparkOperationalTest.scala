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

import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.api.extension.ExtendWith

@deprecated("Deprecating: tests should use the simpler replacement classes (TestResourceCreator, SparkSessionHelper, and possibly - but not required - SparkSimpleTest/SparkOperationalSimpleTest")
@TestInstance(Lifecycle.PER_CLASS)
@ExtendWith(Array(classOf[RequiresOperationalCluster]))
class SparkOperationalTest extends SparkTest {
}

@TestInstance(Lifecycle.PER_CLASS)
@ExtendWith(Array(classOf[RequiresOperationalCluster]))
abstract class SparkOperationalSimpleTest extends SparkSimpleTest {
}
