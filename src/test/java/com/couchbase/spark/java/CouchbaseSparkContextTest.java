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
package com.couchbase.spark.java;

import com.couchbase.client.java.document.JsonDocument;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import java.util.Arrays;

import static com.couchbase.spark.java.CouchbaseRDD.couchbaseRDD;
import static com.couchbase.spark.java.CouchbaseSparkContext.couchbaseContext;

public class CouchbaseSparkContextTest {

    @Test
    public void shouldGetADocument() {
        SparkConf conf = new SparkConf().setAppName("javaTest").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        CouchbaseSparkContext csc = couchbaseContext(sc);


        JavaRDD<JsonDocument> jsonDocumentJavaRDD =
            couchbaseRDD(sc.parallelize(Arrays.asList("airline_2357"))).couchbaseGet("default", JsonDocument.class);


        System.out.println(jsonDocumentJavaRDD.collect());
    }

}
