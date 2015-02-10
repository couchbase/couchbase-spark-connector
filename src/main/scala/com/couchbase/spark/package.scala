package com.couchbase

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

package object spark {

  implicit def toSparkContextFunctions(sc: SparkContext): SparkContextFunctions = new SparkContextFunctions(sc)
  implicit def toRDDFunctions[T](rdd: RDD[T]): RDDFunctions[T] = new RDDFunctions(rdd)

}