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
