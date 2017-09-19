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
import com.couchbase.spark.DocumentRDDFunctions;
import com.couchbase.spark.StoreMode;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import scala.concurrent.duration.Duration;
import scala.reflect.ClassTag;

import java.util.concurrent.TimeUnit;

public class CouchbaseDocumentRDD<T extends Document<?>> extends JavaRDD<T> {

    private final JavaRDD<T> source;

    protected CouchbaseDocumentRDD(JavaRDD<T> source) {
        super(source.rdd(), source.classTag());
        this.source = source;
    }

    public static <T extends Document<?>> CouchbaseDocumentRDD<T> couchbaseDocumentRDD(RDD<T> source) {
        return couchbaseDocumentRDD(source.toJavaRDD());
    }

    public static <T extends Document<?>> CouchbaseDocumentRDD<T> couchbaseDocumentRDD(JavaRDD<T> source) {
        return new CouchbaseDocumentRDD<T>(source);
    }

    public void saveToCouchbase() {
        saveToCouchbase(StoreMode.UPSERT, null);
    }

    public void saveToCouchbase(StoreMode storeMode) {
        saveToCouchbase(storeMode, null);
    }

    public void saveToCouchbase(String bucket) {
        saveToCouchbase(StoreMode.UPSERT, bucket);
    }

    public void saveToCouchbase(StoreMode storeMode, String bucket) {
        new DocumentRDDFunctions<T>(source.rdd()).saveToCouchbase(bucket, storeMode, scala.Option.<Duration>apply(null));
    }

    public void saveToCouchbase(long timeout) {
        saveToCouchbase(StoreMode.UPSERT, null, timeout);
    }

    public void saveToCouchbase(StoreMode storeMode, long timeout) {
        saveToCouchbase(storeMode, null, timeout);
    }

    public void saveToCouchbase(String bucket, long timeout) {
        saveToCouchbase(StoreMode.UPSERT, bucket, timeout);
    }

    public void saveToCouchbase(StoreMode storeMode, String bucket, long timeout) {
        new DocumentRDDFunctions<T>(source.rdd()).saveToCouchbase(bucket, storeMode,
            scala.Option.<Duration>apply(Duration.create(timeout, TimeUnit.MILLISECONDS)));
    }

    @Override
    public RDD<T> rdd() {
        return source.rdd();
    }

    @Override
    public ClassTag<T> classTag() {
        return source.classTag();
    }
}
