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
import com.couchbase.spark.RDDFunctions;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import scala.Predef;
import scala.reflect.ClassTag;

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
     *
     * @param bucket the name of the bucket.
     * @param clazz the target document conversion class.
     */
    @SuppressWarnings({"unchecked"})
    public <D extends Document> JavaRDD<D> couchbaseGet(String bucket, Class<D> clazz) {
        return new RDDFunctions<T>(source.rdd()).couchbaseGet(
            bucket,
            SparkUtil.classTag(clazz),
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
