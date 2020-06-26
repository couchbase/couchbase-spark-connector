/*
 * Copyright (c) 2015 Couchbase, Inc.
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
package com.couchbase.spark.japi;

import com.couchbase.spark.sql.N1QLRelation;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;

import java.util.Map;

public class CouchbaseDataFrameReader {

    private final DataFrameReader dfr;
    private static final String SOURCE = "com.couchbase.spark.sql.DefaultSource";

    protected CouchbaseDataFrameReader(DataFrameReader dfr) {
        this.dfr = dfr;
        dfr.format(SOURCE);
    }

    public static CouchbaseDataFrameReader couchbaseReader(DataFrameReader dfr) {
        return new CouchbaseDataFrameReader(dfr);
    }

    public Dataset<Row> couchbase() {
        prepare(null, null, null);
        return dfr.load();
    }

    public Dataset<Row> couchbase(Map<String, String> options) {
        prepare(options, null, null);
        return dfr.load();
    }

    public Dataset<Row> couchbase(StructType schema, Map<String, String> options) {
        prepare(options, schema, null);
        return dfr.load();
    }

    public Dataset<Row> couchbase(StructType schema) {
        prepare(null, schema, null);
        return dfr.load();
    }

    public Dataset<Row> couchbase(Filter schemaFilter, Map<String, String> options) {
        prepare(options, null, schemaFilter);
        return dfr.load();
    }

    public Dataset<Row> couchbase(Filter schemaFilter) {
        prepare(null, null, schemaFilter);
        return dfr.load();
    }

    public Dataset<Row> couchbase(StructType schema, Filter schemaFilter) {
        prepare(null, schema, schemaFilter);
        return dfr.load();
    }

    public Dataset<Row> couchbase(StructType schema, Filter schemaFilter, Map<String, String> options) {
        prepare(options, schema, schemaFilter);
        return dfr.load();
    }

    private void prepare(Map<String, String> options, StructType schema, Filter schemaFilter) {
        if (schema != null) {
            dfr.schema(schema);
        }
        if (schemaFilter != null) {
            dfr.option("schemaFilter", N1QLRelation.filterToExpression(schemaFilter));
        }

        if (options != null) {
            dfr.options(options);
        }
    }

}
