package com.couchbase.spark.sql

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.types.DataType


class SparkSQLFunctions(@transient val ssc: SQLContext) extends Serializable {

  def n1ql(userSchema: Map[String, DataType], bucketName: String = null): DataFrame = {
    val relation = new N1QLRelation(bucketName, userSchema)(ssc)
    ssc.baseRelationToDataFrame(relation)
  }

}
