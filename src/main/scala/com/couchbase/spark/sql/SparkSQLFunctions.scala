package com.couchbase.spark.sql

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.types.{StructType, DataType}
import org.apache.spark.sql.sources.Filter

class SparkSQLFunctions(@transient val ssc: SQLContext) extends Serializable {

  def n1ql(filter: Filter = null, userSchema: StructType = null, bucketName: String = null): DataFrame = {
    val relation = new N1QLRelation(bucketName, Option(userSchema), Option(filter))(ssc)
    ssc.baseRelationToDataFrame(relation)
  }

}
