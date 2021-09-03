import com.couchbase.spark.kv.KeyValueOptions
import com.couchbase.spark.query.QueryOptions
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object SparkSQL {

  def main(args: Array[String]): Unit = {

    // tag::context[]
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark SQL")
      .config("spark.couchbase.connectionString", "127.0.0.1")
      .config("spark.couchbase.username", "Administrator")
      .config("spark.couchbase.password", "password")
      .config("spark.couchbase.implicitBucket", "travel-sample")
      .getOrCreate()
    // end::context[]

    {
      // tag::simpledf[]
      val queryDf = spark.read.format("couchbase.query").load()

      val analyticsDf = spark.read.format("couchbase.analytics").load()
      // end::simpledf[]
    }

    {
      // tag::queryfilter[]
      val airlines = spark.read
        .format("couchbase.query")
        .option(QueryOptions.Filter, "type = 'airline'")
        .load()
      // end::queryfilter[]

      airlines.printSchema()
    }

    {
      // tag::manualschema[]
      val airlines = spark.read
        .format("couchbase.query")
        .schema(StructType(
          StructField("name", StringType) ::
            StructField("type", StringType) :: Nil
        ))
        .load()
      // end::manualschema[]
    }

    {
      // tag::kvwrite[]
      val airlines = spark.read.format("couchbase.query")
        .option(QueryOptions.Filter, "type = 'airline'")
        .load()
        .limit(5)

      airlines.write.format("couchbase.kv")
        .option(KeyValueOptions.Bucket, "test")
        .option(KeyValueOptions.Durability, KeyValueOptions.MajorityDurability)
        .save()
      // end::kvwrite[]
    }

    {
      // tag::caseclass[]
      case class Airline(name: String, iata: String, icao: String, country: String)
      // end::caseclass[]

      import spark.implicits._

      // tag::ds[]
      val airlines = spark.read.format("couchbase.query")
        .option(QueryOptions.Filter, "type = 'airline'")
        .load()
        .as[Airline]
      // end::ds[]

      // tag::dsfetch[]
      airlines
        .map(_.name)
        .filter(_.toLowerCase.startsWith("a"))
        .foreach(println(_))
      // end::dsfetch[]

    }

  }

}
