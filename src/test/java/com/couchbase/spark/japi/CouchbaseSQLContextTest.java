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

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.sources.EqualTo;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static com.couchbase.spark.japi.CouchbaseDataFrameReader.couchbaseReader;
import static org.junit.Assert.assertEquals;

public class CouchbaseSQLContextTest {

    private static SQLContext sql;

    @BeforeClass
    public static void setup() {
        SparkConf conf = new SparkConf()
            .setAppName("javaTest")
            .setMaster("local[*]")
            .set("com.couchbase.bucket.travel-sample", "");
        SparkSession spark = SparkSession
          .builder()
          .config(conf)
          .getOrCreate();
        sql = spark.sqlContext();
    }

    @Test
    public void shouldQueryViaSQL() {
        Dataset<Row> airlines = couchbaseReader(sql.read()).couchbase(new EqualTo("type", "airline"));
        assertEquals(187, airlines.count());
    }

    @Test
    public void shouldUseDataSet() {
        Dataset<Airport> airports = couchbaseReader(sql.read())
            .couchbase(new EqualTo("type", "airport"))
            .select(new Column("airportname").as("name"))
            .as(Encoders.bean(Airport.class));

        List<Airport> allAirports = airports.collectAsList();
        assertEquals(1968, allAirports.size());
    }

}