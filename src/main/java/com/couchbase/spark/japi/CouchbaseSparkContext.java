/**
 * Copyright (C) 2015 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */
package com.couchbase.spark.japi;

import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.query.Query;
import com.couchbase.client.java.view.SpatialViewQuery;
import com.couchbase.client.java.view.ViewQuery;
import com.couchbase.spark.rdd.CouchbaseQueryRow;
import com.couchbase.spark.rdd.CouchbaseSpatialViewRow;
import com.couchbase.spark.rdd.CouchbaseViewRow;
import com.couchbase.spark.rdd.KeyValueRDD;
import com.couchbase.spark.rdd.QueryRDD;
import com.couchbase.spark.rdd.SpatialViewRDD;
import com.couchbase.spark.rdd.ViewRDD;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.util.List;

public class CouchbaseSparkContext {

    private final SparkContext sc;

    protected CouchbaseSparkContext(SparkContext sc) {
        this.sc = sc;
    }

    public static CouchbaseSparkContext couchbaseContext(SparkContext sc) {
        return new CouchbaseSparkContext(sc);
    }

    public static CouchbaseSparkContext couchbaseContext(JavaSparkContext sc) {
        return new CouchbaseSparkContext(sc.sc());
    }

    public JavaRDD<JsonDocument> couchbaseGet(List<String> ids, String bucket) {
        return couchbaseGet(ids, bucket, JsonDocument.class);
    }

    public JavaRDD<JsonDocument> couchbaseGet(List<String> ids) {
        return couchbaseGet(ids, null, JsonDocument.class);
    }

    public <D extends Document> JavaRDD<D> couchbaseGet(List<String> ids, Class<D> clazz) {
        return couchbaseGet(ids, null, clazz);
    }

    @SuppressWarnings({"unchecked"})
    public <D extends Document> JavaRDD<D> couchbaseGet(List<String> ids, String bucket, Class<D> clazz) {
        return new KeyValueRDD(
            sc,
            SparkUtil.listToSeq(ids),
            bucket,
            SparkUtil.classTag(clazz)
        ).toJavaRDD();
    }

    public JavaRDD<CouchbaseViewRow> couchbaseView(final ViewQuery query) {
        return couchbaseView(query, null);
    }

    public JavaRDD<CouchbaseViewRow> couchbaseView(final ViewQuery query, final String bucket) {
        return new ViewRDD(sc, query, bucket).toJavaRDD();
    }

    public JavaRDD<CouchbaseSpatialViewRow> couchbaseSpatialView(final SpatialViewQuery query) {
        return couchbaseSpatialView(query, null);
    }

    public JavaRDD<CouchbaseSpatialViewRow> couchbaseSpatialView(final SpatialViewQuery query, final String bucket) {
        return new SpatialViewRDD(sc, query, bucket).toJavaRDD();
    }

    public JavaRDD<CouchbaseQueryRow> couchbaseQuery(final Query query) {
        return couchbaseQuery(query, null);
    }

    public JavaRDD<CouchbaseQueryRow> couchbaseQuery(final Query query, final String bucket) {
        return new QueryRDD(sc, query, bucket).toJavaRDD();
    }

}
