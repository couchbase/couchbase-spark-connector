package com.couchbase.spark.sql

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.types.{StructType, DataType}


class SparkSQLFunctions(@transient val ssc: SQLContext) extends Serializable {

  def n1ql(userSchema: StructType = null, bucketName: String = null): DataFrame = {
    val relation = new N1QLRelation(bucketName, Option(userSchema))(ssc)
    ssc.baseRelationToDataFrame(relation)
  }

}
