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

import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.view.SpatialViewQuery;
import com.couchbase.client.java.view.ViewQuery;
import com.couchbase.spark.RDDFunctions;
import com.couchbase.spark.connection.SubdocLookupResult;
import com.couchbase.spark.connection.SubdocLookupSpec;
import com.couchbase.spark.connection.SubdocMutationResult;
import com.couchbase.spark.connection.SubdocMutationSpec;
import com.couchbase.spark.rdd.*;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Option;
import scala.concurrent.duration.Duration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

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
            scala.Option.<Duration>apply(null),
            SparkUtil.classTag(clazz)
        ).toJavaRDD();
    }

    public JavaRDD<JsonDocument> couchbaseGet(List<String> ids, String bucket, long timeout) {
        return couchbaseGet(ids, bucket, JsonDocument.class, timeout);
    }

    public JavaRDD<JsonDocument> couchbaseGet(List<String> ids, long timeout) {
        return couchbaseGet(ids, null, JsonDocument.class, timeout);
    }

    public <D extends Document> JavaRDD<D> couchbaseGet(List<String> ids, Class<D> clazz, long timeout) {
        return couchbaseGet(ids, null, clazz, timeout);
    }

    @SuppressWarnings({"unchecked"})
    public <D extends Document> JavaRDD<D> couchbaseGet(List<String> ids, String bucket, Class<D> clazz, long timeout) {
        return new KeyValueRDD(
            sc,
            SparkUtil.listToSeq(ids),
            bucket,
            scala.Option.<Duration>apply(Duration.create(timeout, TimeUnit.MILLISECONDS)),
            SparkUtil.classTag(clazz)
        ).toJavaRDD();
    }

    public JavaRDD<SubdocLookupResult> couchbaseSubdocLookup(List<String> ids, List<String> get) {
        return couchbaseSubdocLookup(ids, get, Collections.<String>emptyList(), null);
    }

    public JavaRDD<SubdocLookupResult> couchbaseSubdocLookup(List<String> ids, List<String> get, List<String> exists) {
        return couchbaseSubdocLookup(ids, get, exists, null);
    }

    @SuppressWarnings({"unchecked"})
    public JavaRDD<SubdocLookupResult> couchbaseSubdocLookup(List<String> ids, List<String> get, List<String> exists,
        String bucket) {
        List<SubdocLookupSpec> specs = new ArrayList<SubdocLookupSpec>(ids.size());
        for (String id : ids) {
            specs.add(new SubdocLookupSpec(id, SparkUtil.listToSeq(get), SparkUtil.listToSeq(exists)));
        }
        return new SubdocLookupRDD(
            sc,
            SparkUtil.listToSeq(specs),
            bucket,
            scala.Option.<Duration>apply(null)
        ).toJavaRDD();
    }

    public JavaRDD<SubdocLookupResult> couchbaseSubdocLookup(List<String> ids, List<String> get, long timeout) {
        return couchbaseSubdocLookup(ids, get, Collections.<String>emptyList(), null, timeout);
    }

    public JavaRDD<SubdocLookupResult> couchbaseSubdocLookup(List<String> ids, List<String> get, List<String> exists, long timeout) {
        return couchbaseSubdocLookup(ids, get, exists, null, timeout);
    }

    @SuppressWarnings({"unchecked"})
    public JavaRDD<SubdocLookupResult> couchbaseSubdocLookup(List<String> ids, List<String> get, List<String> exists,
                                                             String bucket, long timeout) {
        List<SubdocLookupSpec> specs = new ArrayList<SubdocLookupSpec>(ids.size());
        for (String id : ids) {
            specs.add(new SubdocLookupSpec(id, SparkUtil.listToSeq(get), SparkUtil.listToSeq(exists)));
        }
        return new SubdocLookupRDD(
            sc,
            SparkUtil.listToSeq(specs),
            bucket,
            scala.Option.<Duration>apply(Duration.create(timeout, TimeUnit.MILLISECONDS))
        ).toJavaRDD();
    }

    @SuppressWarnings({"unchecked"})
    public JavaRDD<SubdocMutationResult> couchbaseSubdocMutate(List<SubdocMutationSpec> specs) {
        return couchbaseSubdocMutate(specs, null);
    }

    @SuppressWarnings({"unchecked"})
    public JavaRDD<SubdocMutationResult> couchbaseSubdocMutate(List<SubdocMutationSpec> specs, String bucket) {
        return new SubdocMutateRDD(
            sc,
            SparkUtil.listToSeq(specs),
            bucket,
            scala.Option.<Duration>apply(null)
        ).toJavaRDD();
    }

    @SuppressWarnings({"unchecked"})
    public JavaRDD<SubdocMutationResult> couchbaseSubdocMutate(List<SubdocMutationSpec> specs, long timeout) {
        return couchbaseSubdocMutate(specs, null, timeout);
    }

    @SuppressWarnings({"unchecked"})
    public JavaRDD<SubdocMutationResult> couchbaseSubdocMutate(List<SubdocMutationSpec> specs, String bucket, long timeout) {
        return new SubdocMutateRDD(
            sc,
            SparkUtil.listToSeq(specs),
            bucket,
            scala.Option.<Duration>apply(Duration.create(timeout, TimeUnit.MILLISECONDS))
        ).toJavaRDD();
    }

    public JavaRDD<CouchbaseViewRow> couchbaseView(final ViewQuery query) {
        return couchbaseView(query, null);
    }

    public JavaRDD<CouchbaseViewRow> couchbaseView(final ViewQuery query, long timeout) {
        return couchbaseView(query, null, timeout);
    }

    public JavaRDD<CouchbaseViewRow> couchbaseView(final ViewQuery query, final String bucket) {
        return new ViewRDD(sc, query, bucket, scala.Option.<Duration>apply(null), null).toJavaRDD();
    }

    public JavaRDD<CouchbaseViewRow> couchbaseView(final ViewQuery query, final String bucket, long timeout) {
        return new ViewRDD(sc, query, bucket,
            scala.Option.<Duration>apply(Duration.create(timeout, TimeUnit.MILLISECONDS)),
        null).toJavaRDD();
    }

    public JavaRDD<CouchbaseSpatialViewRow> couchbaseSpatialView(final SpatialViewQuery query) {
        return couchbaseSpatialView(query, null);
    }

    public JavaRDD<CouchbaseSpatialViewRow> couchbaseSpatialView(final SpatialViewQuery query, final String bucket) {
        return new SpatialViewRDD(sc, query, bucket, scala.Option.<Duration>apply(null)).toJavaRDD();
    }

    public JavaRDD<CouchbaseSpatialViewRow> couchbaseSpatialView(final SpatialViewQuery query, long timeout) {
        return couchbaseSpatialView(query, null);
    }

    public JavaRDD<CouchbaseSpatialViewRow> couchbaseSpatialView(final SpatialViewQuery query, final String bucket, long timeout) {
        return new SpatialViewRDD(sc, query, bucket, scala.Option.<Duration>apply(Duration.create(timeout, TimeUnit.MILLISECONDS))).toJavaRDD();
    }

    public JavaRDD<CouchbaseQueryRow> couchbaseQuery(final N1qlQuery query) {
        return couchbaseQuery(query, null);
    }

    public JavaRDD<CouchbaseQueryRow> couchbaseQuery(final N1qlQuery query, final String bucket) {
        return new QueryRDD(sc, query, bucket, scala.Option.<Duration>apply(null)).toJavaRDD();
    }

    public JavaRDD<CouchbaseQueryRow> couchbaseQuery(final N1qlQuery query, long timeout) {
        return couchbaseQuery(query, null);
    }

    public JavaRDD<CouchbaseQueryRow> couchbaseQuery(final N1qlQuery query, final String bucket, long timeout) {
        return new QueryRDD(sc, query, bucket, scala.Option.<Duration>apply(Duration.create(timeout, TimeUnit.MILLISECONDS))).toJavaRDD();
    }

}
