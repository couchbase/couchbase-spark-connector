package com.couchbase.spark.java;

import com.couchbase.client.java.document.JsonDocument;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;
import java.util.Arrays;

import static com.couchbase.spark.java.CouchbaseSparkContext.javaFunctions;

/**
 * .
 *
 * @author Michael Nitschinger
 */
public class CouchbaseSparkContextTest {

    @Test
    public void shouldGetADocument() {
        SparkConf conf = new SparkConf().setAppName("javaTest").setMaster("local[*]");
        SparkContext sc = new SparkContext(conf);
        CouchbaseSparkContext csc = javaFunctions(sc);

        JavaRDD<JsonDocument> docs = csc.couchbaseGet(Arrays.asList("airline_2357"));

        System.out.println(docs.collect());
    }

}
