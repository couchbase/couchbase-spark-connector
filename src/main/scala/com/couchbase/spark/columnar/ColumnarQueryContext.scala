package com.couchbase.spark.columnar

// Not using .bucket or .scope as those are for ns-server scopes and collections, and analyticsQuery should
// not have been added to Scope.
// We also don't use any implicit buckets and scopes provided in the config, for similar reasons.
// Columnar databases, scopes and collections are very different to ns-server buckets, scopes and collections, and
// we separate them accordingly.
object ColumnarQueryContext {
  def queryContext(databaseName: String, scopeName: String): String = {
    s"default:`${databaseName}`.`${scopeName}`"
  }

}
