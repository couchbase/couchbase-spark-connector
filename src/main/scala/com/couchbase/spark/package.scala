package com.couchbase

import org.apache.spark.SparkContext

package object spark {
  implicit def toSparkContextFunctions(sc: SparkContext): SparkContextFunctions =
    new SparkContextFunctions(sc)
}
