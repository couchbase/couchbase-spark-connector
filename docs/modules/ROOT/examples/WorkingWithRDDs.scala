import com.couchbase.client.scala.json.JsonObject
import com.couchbase.spark.kv.{Get, Upsert}
import org.apache.spark.sql.SparkSession

object WorkingWithRDDs {

  def main(args: Array[String]): Unit = {

    // tag::context[]
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("WorkingWithRDDs")
      .config("spark.couchbase.connectionString", "127.0.0.1")
      .config("spark.couchbase.username", "Administrator")
      .config("spark.couchbase.password", "password")
      .config("spark.couchbase.implicitBucket", "travel-sample")
      .getOrCreate()
    // end::context[]

    // tag::import[]
    import com.couchbase.spark._
    // end::import[]

    // tag::get[]
    spark
      .sparkContext
      .couchbaseGet(Seq(Get("airline_10"), Get("airline_10642")))
      .collect()
      .foreach(result => println(result.contentAs[JsonObject]))
    // end::get[]

    // tag::query[]
    spark
      .sparkContext
      .couchbaseQuery[JsonObject]("select count(*) as count from `travel-sample`")
      .collect()
      .foreach(println)
    // end::query[]

    // tag::upsert[]
    spark
      .sparkContext
      .couchbaseGet(Seq(Get("airline_10"), Get("airline_10642"), Get("airline_10748")))
      .map(getResult => Upsert(getResult.id, getResult.contentAs[JsonObject].get))
      .couchbaseUpsert(Keyspace(bucket = Some("targetBucket")))
      .collect()
      .foreach(println)
    // end::upsert[]
  }
}
