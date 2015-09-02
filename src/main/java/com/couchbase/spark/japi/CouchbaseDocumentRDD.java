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
import com.couchbase.spark.DocumentRDDFunctions;
import com.couchbase.spark.StoreMode;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import scala.reflect.ClassTag;

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
        new DocumentRDDFunctions<T>(source.rdd()).saveToCouchbase(bucket, storeMode);
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
