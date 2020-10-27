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
package com.couchbase.spark.sql

import com.couchbase.spark.sql.N1QLRelation.filterToExpression
import org.apache.spark.sql.sources._
import org.junit.runner.RunWith
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FilterSpec extends AnyFlatSpec with Matchers {

  "The Filter" should "convert EqualTo" in {
    filterToExpression(EqualTo("foo", "bar")) should equal(" `foo` = 'bar'")
    filterToExpression(EqualTo("foo", 1)) should equal(" `foo` = 1")
  }

  it should "convert GreaterThan" in {
    filterToExpression(GreaterThan("foo", "bar")) should equal(" `foo` > 'bar'")
    filterToExpression(GreaterThan("foo", 1)) should equal(" `foo` > 1")
  }

  it should "convert GreaterThanOrEqual" in {
    filterToExpression(GreaterThanOrEqual("foo", "bar")) should equal(" `foo` >= 'bar'")
    filterToExpression(GreaterThanOrEqual("foo", 1)) should equal(" `foo` >= 1")
  }

  it should "convert LessThan" in {
    filterToExpression(LessThan("foo", "bar")) should equal(" `foo` < 'bar'")
    filterToExpression(LessThan("foo", 1)) should equal(" `foo` < 1")
  }

  it should "convert LessThanOrEqual" in {
    filterToExpression(LessThanOrEqual("foo", "bar")) should equal(" `foo` <= 'bar'")
    filterToExpression(LessThanOrEqual("foo", 1)) should equal(" `foo` <= 1")
  }

  it should "convert IsNull" in {
    filterToExpression(IsNull("foo")) should equal(" `foo` IS NULL")
  }

  it should "convert IsNotNull" in {
    filterToExpression(IsNotNull("foo")) should equal(" `foo` IS NOT NULL")
  }

  it should "convert StringContains" in {
    filterToExpression(StringContains("foo", "bar")) should equal(" CONTAINS(`foo`, 'bar')")
  }

  it should "convert StringStartsWith" in {
    filterToExpression(StringStartsWith("foo", "bar")) should equal(" `foo` LIKE 'bar%'")
  }

  it should "convert StringEndsWith" in {
    filterToExpression(StringEndsWith("foo", "bar")) should equal(" `foo` LIKE '%bar'")
  }

  it should "escape . for StringStartsWith" in {
    filterToExpression(StringStartsWith("foo", "b.ar")) should equal(" `foo` LIKE 'b\\.ar%'")
  }

  it should "escape * for StringStartsWith" in {
    filterToExpression(StringStartsWith("foo", "b*ar")) should equal(" `foo` LIKE 'b\\*ar%'")
  }

  it should "escape . for StringEndsWith" in {
    filterToExpression(StringEndsWith("foo", "b.ar")) should equal(" `foo` LIKE '%b\\.ar'")
  }

  it should "escape * for StringEndsWith" in {
    filterToExpression(StringEndsWith("foo", "b*ar")) should equal(" `foo` LIKE '%b\\*ar'")
  }

  it should "convert In" in {
    filterToExpression(In("foo", Array("blub", 1, true))) should equal (" `foo` IN ['blub',1,true]")
  }

  it should "convert And" in {
    val expr = And(EqualTo("type", "airline"), IsNotNull("name"))
    filterToExpression(expr) should equal (" ( `type` = 'airline' AND  `name` IS NOT NULL)")
  }

  it should "convert Or" in {
    val expr = Or(EqualTo("type", "airline"), EqualTo("type", "airport"))
    filterToExpression(expr) should equal (" ( `type` = 'airline' OR  `type` = 'airport')")
  }

  it should "convert Not" in {
    val expr = Not(And(IsNull("name"), IsNull("age")))
    filterToExpression(expr) should equal (" NOT ( ( `name` IS NULL AND  `age` IS NULL))")
  }

  it should "parse paths in nested filter attributes" in {
    val filter = EqualTo("flavour.type", "10")

    val parsedFilter = N1QLRelation.filterToExpression(filter)

    parsedFilter should equal(" `flavour`.`type` = '10'")
  }

  it should "parse paths without nested filter attributes" in {
    val filter = EqualTo("type", "10")

    val parsedFilter = N1QLRelation.filterToExpression(filter)

    parsedFilter should equal(" `type` = '10'")
  }

  it should "parse deeply filter attributes" in {
    val filter = EqualTo("flavour.origin.country.region", "tuscany")

    val parsedFilter = N1QLRelation.filterToExpression(filter)

    parsedFilter should equal(" `flavour`.`origin`.`country`.`region` = 'tuscany'")
  }

  it should "not parse text marked as verbatim" in {
    val filter = EqualTo("'substr('textField',0,10)'", "2016-09-13")

    val parsedFilter = N1QLRelation.filterToExpression(filter)

    parsedFilter should equal(" substr('textField',0,10) = '2016-09-13'")
  }
}
