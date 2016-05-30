package com.couchbase.spark.sql

import org.apache.spark.sql.sources.{And, EqualTo, LessThan}
import org.scalatest.{FlatSpec, Matchers}
import org.scalatest._

/**
  * Created by luca on 30/05/16.
  */
class N1QLRelationSpec extends FlatSpec with Matchers {

  "N1QLRelation" should "parse paths in nested filter attributes" in {
    val filter = EqualTo("flavour.type", "10")

    val parsedFilter = N1QLRelation.filterToExpression(filter)

    parsedFilter should equal(" `flavour`.`type` = '10'")
  }

  "N1QLRelation" should "not parse paths in nested filter attributes" in {
    val filter = EqualTo("type", "10")

    val parsedFilter = N1QLRelation.filterToExpression(filter)

    parsedFilter should equal(" `type` = '10'")
  }

  "N1QLRelation" should "parse complex filter attributes" in {
    val filter = And(EqualTo("type", "10"),LessThan("flavour.origin", 4))

    val parsedFilter = N1QLRelation.filterToExpression(filter)

    parsedFilter should equal(" ( `type` = '10' AND  `flavour`.`origin` < 4)")
  }
}
