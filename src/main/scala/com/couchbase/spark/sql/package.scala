package com.couchbase.spark

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.Filter

package object sql {

  implicit def toSparkSQLFunctions(ssc: SQLContext): SparkSQLFunctions = new SparkSQLFunctions(ssc)

}
