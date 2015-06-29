package com.couchbase.spark.java;

import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.query.Query;
import com.couchbase.client.java.view.SpatialViewQuery;
import com.couchbase.client.java.view.ViewQuery;
import com.couchbase.spark.rdd.CouchbaseQueryRow;
import com.couchbase.spark.rdd.CouchbaseSpatialViewRow;
import com.couchbase.spark.rdd.CouchbaseViewRow;
import com.couchbase.spark.rdd.KeyValueRDD;
import com.couchbase.spark.rdd.QueryRDD;
import com.couchbase.spark.rdd.SpatialViewRDD;
import com.couchbase.spark.rdd.ViewRDD;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import scala.collection.Seq;
import scala.reflect.ClassTag;
import java.util.List;

public class CouchbaseSparkContext {

    private final SparkContext sc;

    protected CouchbaseSparkContext(SparkContext sc) {
        this.sc = sc;
    }

    public static CouchbaseSparkContext javaFunctions(SparkContext sc) {
        return new CouchbaseSparkContext(sc);
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
        ClassTag<D> classTag = scala.reflect.ClassTag$.MODULE$.apply(clazz);
        Seq<String> idSeq = scala.collection.JavaConversions.asScalaBuffer(ids).seq();
        return new KeyValueRDD(sc, idSeq, bucket, classTag).toJavaRDD();
    }

    public JavaRDD<CouchbaseViewRow> couchbaseView(final ViewQuery query) {
        return couchbaseView(query, null);
    }

    public JavaRDD<CouchbaseViewRow> couchbaseView(final ViewQuery query, final String bucket) {
        return new ViewRDD(sc, query, bucket).toJavaRDD();
    }

    public JavaRDD<CouchbaseSpatialViewRow> couchbaseSpatialView(final SpatialViewQuery query) {
        return couchbaseSpatialView(query, null);
    }

    public JavaRDD<CouchbaseSpatialViewRow> couchbaseSpatialView(final SpatialViewQuery query, final String bucket) {
        return new SpatialViewRDD(sc, query, bucket).toJavaRDD();
    }

    public JavaRDD<CouchbaseQueryRow> couchbaseQuery(final Query query) {
        return couchbaseQuery(query, null);
    }

    public JavaRDD<CouchbaseQueryRow> couchbaseQuery(final Query query, final String bucket) {
        return new QueryRDD(sc, query, bucket).toJavaRDD();
    }

}
