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
package com.couchbase.spark.internal

import org.junit.runner.RunWith
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class LazyIteratorSpec extends AnyFlatSpec with Matchers {

  "A LazyIterator" should "not create the delegated Iterator in the constructor" in {
    var created = false
    val iter = LazyIterator {
      created = true
      Iterator(1, 2, 3)
    }

    created should equal (false)
    iter.toList should equal (1 :: 2 :: 3 :: Nil)
    created should equal (true)
  }

}
