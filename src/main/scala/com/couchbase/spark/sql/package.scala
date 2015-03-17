package com.couchbase.spark

import org.apache.spark.sql.SQLContext

package object sql {

  implicit def toSparkSQLFunctions(ssc: SQLContext): SparkSQLFunctions = new SparkSQLFunctions(ssc)

}
