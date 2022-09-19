package com.couchbase.spark.config

case class DSConfig(
                        bucket: String,
                        scope: Option[String],
                        collection: Option[String],
                        idFieldName: String,
                        userFilter: Option[String],
                        scanConsistency: String,
                        timeout: Option[String],
                        pushDownAggregate: Boolean,
                        durability: Option[String],
                        dataset: String,
                      )
