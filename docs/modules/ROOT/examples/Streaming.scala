import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object SparkSQL {

  def main(args: Array[String]): Unit = {
    // tag::context[]
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Structured Streaming Example")
      .config("spark.couchbase.connectionString", "127.0.0.1")
      .config("spark.couchbase.username", "Administrator")
      .config("spark.couchbase.password", "password")
      .config("spark.couchbase.implicitBucket", "travel-sample")
      .getOrCreate()
    // end::context[]

    {
      // tag::basicSource[]
      val sourceDf = spark
        .readStream
        .format("couchbase.kv")
        .load
      // end::basicSource[]

      // tag::consoleOnce[]
      val query = sourceDf.writeStream
        .format("console")
        .trigger(Trigger.Once())
        .queryName("kv2console")
        .start

      query.awaitTermination()
      // end::consoleOnce[]
    }

  }

}