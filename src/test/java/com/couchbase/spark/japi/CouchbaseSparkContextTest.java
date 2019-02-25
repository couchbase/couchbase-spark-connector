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

import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.spark.connection.SubdocLookupResult;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.BeforeClass;
import org.junit.Test;
import java.util.Arrays;
import java.util.List;

import static com.couchbase.spark.japi.CouchbaseSparkContext.couchbaseContext;
import static org.junit.Assert.assertEquals;

public class CouchbaseSparkContextTest {

    private static CouchbaseSparkContext csc;

    @BeforeClass
    public static void setup() {
        SparkConf conf = new SparkConf()
            .setAppName("javaTest")
            .setMaster("local[*]")
            .set("com.couchbase.username", "Administrator")
            .set("com.couchbase.password", "password")
            .set("com.couchbase.bucket.travel-sample", "");

        JavaSparkContext sc = new JavaSparkContext(conf);
        csc = couchbaseContext(sc);
    }

    @Test
    public void shouldGetADocument() {
        String id = "airline_2357";

        List<JsonDocument> found = csc
            .couchbaseGet(Arrays.asList(id))
            .collect();

        assertEquals(1, found.size());
        assertEquals(id, found.get(0).id());
        assertEquals("First Choice Airways", found.get(0).content().getString("name"));
    }

    @Test
    public void shouldGetViaSubdoc() {
        String id = "airline_2357";

        List<SubdocLookupResult> found = csc
            .couchbaseSubdocLookup(Arrays.asList(id), Arrays.asList("name"))
            .collect();

        System.out.println(found);
    }

}
