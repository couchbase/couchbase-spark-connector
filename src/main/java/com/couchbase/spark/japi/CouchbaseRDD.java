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
import com.couchbase.spark.RDDFunctions;
import com.couchbase.spark.connection.SubdocLookupResult;
import com.couchbase.spark.connection.SubdocMutationResult;
import com.couchbase.spark.connection.SubdocMutationSpec;
import com.couchbase.spark.rdd.CouchbaseQueryRow;
import com.couchbase.spark.rdd.CouchbaseSpatialViewRow;
import com.couchbase.spark.rdd.CouchbaseViewRow;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import scala.Predef;
import scala.concurrent.duration.Duration;
import scala.reflect.ClassTag;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class CouchbaseRDD<T> extends JavaRDD<T> {

    private final JavaRDD<T> source;

    private CouchbaseRDD(JavaRDD<T> source, ClassTag<T> classTag) {
        super(source.rdd(), classTag);
        this.source = source;
    }

    public static <T> CouchbaseRDD<T> couchbaseRDD(RDD<T> source) {
        return couchbaseRDD(source.toJavaRDD());
    }

    public static <T> CouchbaseRDD<T> couchbaseRDD(JavaRDD<T> source) {
        return new CouchbaseRDD<T>(source, source.classTag());
    }

    /**
     * Loads documents specified by the given document IDs.
     *
     * Note that this method can only be called from an RDD with type String, where the strings
     * are the document IDs. Java is not able to check the instance of the generic type because
     * of type erasure the same way as scala does.
     */
    @SuppressWarnings({"unchecked"})
    public JavaRDD<JsonDocument> couchbaseGet() {
        return couchbaseGet(null, JsonDocument.class);
    }

    /**
     * Loads documents specified by the given document IDs.
     *
     * Note that this method can only be called from an RDD with type String, where the strings
     * are the document IDs. Java is not able to check the instance of the generic type because
     * of type erasure the same way as scala does.
     *
     * @param bucket the name of the bucket.
     */
    @SuppressWarnings({"unchecked"})
    public JavaRDD<JsonDocument> couchbaseGet(String bucket) {
        return couchbaseGet(bucket, JsonDocument.class);
    }

    /**
     * Loads documents specified by the given document IDs.
     *
     * Note that this method can only be called from an RDD with type String, where the strings
     * are the document IDs. Java is not able to check the instance of the generic type because
     * of type erasure the same way as scala does.
     *
     * @param clazz the target document conversion class.
     */
    @SuppressWarnings({"unchecked"})
    public <D extends Document> JavaRDD<D> couchbaseGet(Class<D> clazz) {
        return couchbaseGet(null, clazz);
    }

    /**
     * Loads documents specified by the given document IDs.
     *
     * Note that this method can only be called from an RDD with type String, where the strings
     * are the document IDs. Java is not able to check the instance of the generic type because
     * of type erasure the same way as scala does.
     */
    @SuppressWarnings({"unchecked"})
    public JavaRDD<JsonDocument> couchbaseGet(long timeout) {
        return couchbaseGet(null, JsonDocument.class, timeout);
    }

    /**
     * Loads documents specified by the given document IDs.
     *
     * Note that this method can only be called from an RDD with type String, where the strings
     * are the document IDs. Java is not able to check the instance of the generic type because
     * of type erasure the same way as scala does.
     *
     * @param bucket the name of the bucket.
     */
    @SuppressWarnings({"unchecked"})
    public JavaRDD<JsonDocument> couchbaseGet(String bucket, long timeout) {
        return couchbaseGet(bucket, JsonDocument.class, timeout);
    }

    /**
     * Loads documents specified by the given document IDs.
     *
     * Note that this method can only be called from an RDD with type String, where the strings
     * are the document IDs. Java is not able to check the instance of the generic type because
     * of type erasure the same way as scala does.
     *
     * @param clazz the target document conversion class.
     */
    @SuppressWarnings({"unchecked"})
    public <D extends Document> JavaRDD<D> couchbaseGet(Class<D> clazz, long timeout) {
        return couchbaseGet(null, clazz, timeout);
    }

    /**
     * Loads documents specified by the given document IDs.
     *
     * Note that this method can only be called from an RDD with type String, where the strings
     * are the document IDs. Java is not able to check the instance of the generic type because
     * of type erasure the same way as scala does.
     *
     * @param bucket the name of the bucket.
     * @param clazz the target document conversion class.
     */
    @SuppressWarnings({"unchecked"})
    public <D extends Document> JavaRDD<D> couchbaseGet(String bucket, Class<D> clazz) {
        return new RDDFunctions<T>(source.rdd()).couchbaseGet(
            bucket,
            scala.Option.<Duration>apply(null),
            SparkUtil.classTag(clazz),
            LCLIdentity.INSTANCE
        ).toJavaRDD();
    }

    /**
     * Loads documents specified by the given document IDs.
     *
     * Note that this method can only be called from an RDD with type String, where the strings
     * are the document IDs. Java is not able to check the instance of the generic type because
     * of type erasure the same way as scala does.
     *
     * @param bucket the name of the bucket.
     * @param clazz the target document conversion class.
     */
    @SuppressWarnings({"unchecked"})
    public <D extends Document> JavaRDD<D> couchbaseGet(String bucket, Class<D> clazz, long timeout) {
        return new RDDFunctions<T>(source.rdd()).couchbaseGet(
            bucket,
            scala.Option.<Duration>apply(Duration.create(timeout, TimeUnit.MILLISECONDS)),
            SparkUtil.classTag(clazz),
            LCLIdentity.INSTANCE
        ).toJavaRDD();
    }

    @SuppressWarnings({"unchecked"})
    public JavaRDD<SubdocLookupResult> couchbaseSubdocLookup(List<String> get) {
        return couchbaseSubdocLookup(get, Collections.<String>emptyList(), null);
    }

    @SuppressWarnings({"unchecked"})
    public JavaRDD<SubdocLookupResult> couchbaseSubdocLookup(List<String> get, List<String> exists) {
        return couchbaseSubdocLookup(get, exists, null);
    }

    @SuppressWarnings({"unchecked"})
    public JavaRDD<SubdocLookupResult> couchbaseSubdocLookup(List<String> get, List<String> exists, String bucket) {
        return new RDDFunctions<T>(source.rdd()).couchbaseSubdocLookup(
            SparkUtil.listToSeq(get),
            SparkUtil.listToSeq(exists),
            bucket,
            scala.Option.<Duration>apply(null),
            LCLIdentity.INSTANCE
        ).toJavaRDD();
    }

    @SuppressWarnings({"unchecked"})
    public JavaRDD<SubdocLookupResult> couchbaseSubdocLookup(List<String> get, long timeout) {
        return couchbaseSubdocLookup(get, Collections.<String>emptyList(), null, timeout);
    }

    @SuppressWarnings({"unchecked"})
    public JavaRDD<SubdocLookupResult> couchbaseSubdocLookup(List<String> get, List<String> exists, long timeout) {
        return couchbaseSubdocLookup(get, exists, null, timeout);
    }

    @SuppressWarnings({"unchecked"})
    public JavaRDD<SubdocLookupResult> couchbaseSubdocLookup(List<String> get, List<String> exists, String bucket, long timeout) {
        return new RDDFunctions<T>(source.rdd()).couchbaseSubdocLookup(
            SparkUtil.listToSeq(get),
            SparkUtil.listToSeq(exists),
            bucket,
            scala.Option.<Duration>apply(Duration.create(timeout, TimeUnit.MILLISECONDS)),
            LCLIdentity.INSTANCE
        ).toJavaRDD();
    }

    @SuppressWarnings({"unchecked"})
    public JavaRDD<SubdocMutationResult> couchbaseSubdocMutate(List<SubdocMutationSpec> specs) {
        return couchbaseSubdocMutate(specs, null);
    }

    @SuppressWarnings({"unchecked"})
    public JavaRDD<SubdocMutationResult> couchbaseSubdocMutate(List<SubdocMutationSpec> specs, String bucket) {
        return new RDDFunctions<T>(source.rdd()).couchbaseSubdocMutate(
            SparkUtil.listToSeq(specs),
            bucket,
            scala.Option.<Duration>apply(null),
            LCLIdentity.INSTANCE
        ).toJavaRDD();
    }

    @SuppressWarnings({"unchecked"})
    public JavaRDD<SubdocMutationResult> couchbaseSubdocMutate(List<SubdocMutationSpec> specs, long timeout) {
        return couchbaseSubdocMutate(specs, null, timeout);
    }

    @SuppressWarnings({"unchecked"})
    public JavaRDD<SubdocMutationResult> couchbaseSubdocMutate(List<SubdocMutationSpec> specs, String bucket, long timeout) {
        return new RDDFunctions<T>(source.rdd()).couchbaseSubdocMutate(
            SparkUtil.listToSeq(specs),
            bucket,
            scala.Option.<Duration>apply(Duration.create(timeout, TimeUnit.MILLISECONDS)),
            LCLIdentity.INSTANCE
        ).toJavaRDD();
    }

    @Override
    public RDD<T> rdd() {
        return source.rdd();
    }

    @Override
    public ClassTag<T> classTag() {
        return source.classTag();
    }

    @SuppressWarnings({"unchecked"})
    public JavaRDD<CouchbaseViewRow> couchbaseView() {
        return new RDDFunctions<T>(source.rdd()).couchbaseView(null, scala.Option.<Duration>apply(null), LCLIdentity.INSTANCE).toJavaRDD();
    }

    @SuppressWarnings({"unchecked"})
    public JavaRDD<CouchbaseViewRow> couchbaseView(String bucket) {
        return new RDDFunctions<T>(source.rdd()).couchbaseView(bucket, scala.Option.<Duration>apply(null), LCLIdentity.INSTANCE).toJavaRDD();
    }

    @SuppressWarnings({"unchecked"})
    public JavaRDD<CouchbaseSpatialViewRow> couchbaseSpatialView() {
        return new RDDFunctions<T>(source.rdd()).couchbaseSpatialView(null, scala.Option.<Duration>apply(null), LCLIdentity.INSTANCE).toJavaRDD();
    }

    @SuppressWarnings({"unchecked"})
    public JavaRDD<CouchbaseSpatialViewRow> couchbaseSpatialView(String bucket) {
        return new RDDFunctions<T>(source.rdd()).couchbaseSpatialView(bucket, scala.Option.<Duration>apply(null), LCLIdentity.INSTANCE).toJavaRDD();
    }

    @SuppressWarnings({"unchecked"})
    public JavaRDD<CouchbaseQueryRow> couchbaseQuery() {
        return new RDDFunctions<T>(source.rdd()).couchbaseQuery(null, scala.Option.<Duration>apply(null), LCLIdentity.INSTANCE).toJavaRDD();
    }

    @SuppressWarnings({"unchecked"})
    public JavaRDD<CouchbaseQueryRow> couchbaseQuery(String bucket) {
        return new RDDFunctions<T>(source.rdd()).couchbaseQuery(bucket, scala.Option.<Duration>apply(null), LCLIdentity.INSTANCE).toJavaRDD();
    }

    @SuppressWarnings({"unchecked"})
    public JavaRDD<CouchbaseViewRow> couchbaseView(long timeout) {
        return new RDDFunctions<T>(source.rdd()).couchbaseView(null, scala.Option.<Duration>apply(Duration.create(timeout, TimeUnit.MILLISECONDS)), LCLIdentity.INSTANCE).toJavaRDD();
    }

    @SuppressWarnings({"unchecked"})
    public JavaRDD<CouchbaseViewRow> couchbaseView(String bucket, long timeout) {
        return new RDDFunctions<T>(source.rdd()).couchbaseView(bucket, scala.Option.<Duration>apply(Duration.create(timeout, TimeUnit.MILLISECONDS)), LCLIdentity.INSTANCE).toJavaRDD();
    }

    @SuppressWarnings({"unchecked"})
    public JavaRDD<CouchbaseSpatialViewRow> couchbaseSpatialView(long timeout) {
        return new RDDFunctions<T>(source.rdd()).couchbaseSpatialView(null, scala.Option.<Duration>apply(Duration.create(timeout, TimeUnit.MILLISECONDS)), LCLIdentity.INSTANCE).toJavaRDD();
    }

    @SuppressWarnings({"unchecked"})
    public JavaRDD<CouchbaseSpatialViewRow> couchbaseSpatialView(String bucket, long timeout) {
        return new RDDFunctions<T>(source.rdd()).couchbaseSpatialView(bucket, scala.Option.<Duration>apply(Duration.create(timeout, TimeUnit.MILLISECONDS)), LCLIdentity.INSTANCE).toJavaRDD();
    }

    @SuppressWarnings({"unchecked"})
    public JavaRDD<CouchbaseQueryRow> couchbaseQuery(long timeout) {
        return new RDDFunctions<T>(source.rdd()).couchbaseQuery(null, scala.Option.<Duration>apply(Duration.create(timeout, TimeUnit.MILLISECONDS)), LCLIdentity.INSTANCE).toJavaRDD();
    }

    @SuppressWarnings({"unchecked"})
    public JavaRDD<CouchbaseQueryRow> couchbaseQuery(String bucket, long timeout) {
        return new RDDFunctions<T>(source.rdd()).couchbaseQuery(bucket, scala.Option.<Duration>apply(Duration.create(timeout, TimeUnit.MILLISECONDS)), LCLIdentity.INSTANCE).toJavaRDD();
    }

    /**
     * Calling scala from java is a mess.
     *
     * We'd be better off implementing the java interfaces from scala, at a later point.
     */
    private static class LCLIdentity extends Predef.$less$colon$less {

        public static LCLIdentity INSTANCE = new LCLIdentity();

        @Override
        public Object apply(Object v1) {
            return v1;
        }
    }
}
