/*
 * Copyright (c) 2021 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.spark.query

import com.couchbase.client.scala.codec.JsonDeserializer.Passthrough
import com.couchbase.spark.config.{CouchbaseConfig, CouchbaseConnection}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import com.couchbase.client.scala.query.{QueryScanConsistency, QueryOptions => CouchbaseQueryOptions}
import com.couchbase.spark.DefaultConstants
import com.couchbase.spark.json.CouchbaseJsonUtils

import scala.concurrent.duration.Duration

class QueryPartitionReader(schema: StructType, conf: CouchbaseConfig, readConfig: QueryReadConfig, filters: Array[Filter])
  extends PartitionReader[InternalRow]
  with Logging {

  private val scopeName = readConfig.scope.getOrElse(DefaultConstants.DefaultScopeName)
  private val collectionName = readConfig.collection.getOrElse(DefaultConstants.DefaultCollectionName)

  private val parser = CouchbaseJsonUtils.jsonParser(schema)
  private val createParser = CouchbaseJsonUtils.createParser()

  private lazy val result = {
    if (scopeName.equals(DefaultConstants.DefaultScopeName) && collectionName.equals(DefaultConstants.DefaultCollectionName)) {
      CouchbaseConnection().cluster(conf).query(buildQuery(), buildOptions())
    } else {
      CouchbaseConnection().cluster(conf).bucket(readConfig.bucket).scope(scopeName).query(buildQuery(), buildOptions())
    }
  }

  private lazy val rows = result
    .flatMap(result => result.rowsAs[String](Passthrough.StringConvert))
    .get
    .flatMap(row => {
      try {
        parser.parse(row, createParser, UTF8String.fromString)
      } catch {
        case e: Exception =>
          throw new IllegalStateException(s"Could not parse row $row based on provided schema $schema.", e)
      }
    })

  private lazy val rowIterator = rows.iterator
  protected var lastRow: InternalRow = InternalRow()

  def buildQuery(): String = {
    var fields = schema
      .fields
      .map(f => f.name)
      .filter(f => !f.equals(readConfig.idFieldName))

    var predicate = readConfig.userFilter.map(p => s" WHERE $p").getOrElse("")
    val compiledFilters = N1qlFilters.compile(filters)
    if (compiledFilters.nonEmpty && predicate.nonEmpty) {
      predicate = predicate + " AND " + compiledFilters
    } else if (compiledFilters.nonEmpty) {
      predicate = " WHERE " + compiledFilters
    }

    fields = fields :+ s"META().id as ${readConfig.idFieldName}"
    val fieldsEncoded = fields.mkString(", ")

    val query = if (scopeName.equals(DefaultConstants.DefaultScopeName) && collectionName.equals(DefaultConstants.DefaultCollectionName)) {
      s"select $fieldsEncoded from `${readConfig.bucket}`$predicate"
    } else {
      s"select $fieldsEncoded from `$collectionName`$predicate"
    }

    logDebug(s"Building and running N1QL query $query")
    query
  }

  def buildOptions(): CouchbaseQueryOptions = {
    var opts = CouchbaseQueryOptions()
    readConfig.scanConsistency match {
      case QueryOptions.NotBoundedScanConsistency => opts = opts.scanConsistency(QueryScanConsistency.NotBounded)
      case QueryOptions.RequestPlusScanConsistency => opts = opts.scanConsistency(QueryScanConsistency.RequestPlus())
      case v => throw new IllegalArgumentException("Unknown scanConsistency of " + v)
    }
    readConfig.timeout.foreach(t => opts = opts.timeout(Duration(t)))
    opts
  }

  override def next(): Boolean = {
    if (rowIterator.hasNext) {
      lastRow = rowIterator.next()
      true
    } else {
      false
    }
  }

  override def get(): InternalRow = lastRow
  override def close(): Unit = {}

}
