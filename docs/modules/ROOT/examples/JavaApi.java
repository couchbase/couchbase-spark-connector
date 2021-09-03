import com.couchbase.spark.query.QueryOptions;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;

public class JavaApi {

  public static void main(String... args) {
    // tag::context[]
    SparkSession spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Java API")
      .config("spark.couchbase.connectionString", "127.0.0.1")
      .config("spark.couchbase.username", "Administrator")
      .config("spark.couchbase.password", "password")
      .config("spark.couchbase.implicitBucket", "travel-sample")
      .getOrCreate();
      // end::context[]

    {
      // tag::dfread[]
      Dataset<Row> airlines = spark.read()
        .format("couchbase.query")
        .option(QueryOptions.Filter(), "type = 'airline'")
        .option(QueryOptions.Bucket(), "travel-sample")
        .load();

      airlines.show(3);
      // end::dfread[]
    }


    {
      // tag::dsread[]
      Dataset<Airline> airlines = spark.read()
        .format("couchbase.query")
        .option(QueryOptions.Filter(), "type = 'airline'")
        .option(QueryOptions.Bucket(), "travel-sample")
        .load()
        .as(Encoders.bean(Airline.class));

      airlines
        .limit(3)
        .foreach(airline -> {
          System.out.println("Airline:" + airline);
        });
      // end::dsread[]
    }


  }

  // tag::airportclass[]
  public static class Airline implements Serializable {

    private String name;
    private String callsign;
    private String country;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getCallsign() {
      return callsign;
    }

    public void setCallsign(String callsign) {
      this.callsign = callsign;
    }

    public String getCountry() {
      return country;
    }

    public void setCountry(String country) {
      this.country = country;
    }

    @Override
    public String toString() {
      return "Airline{" +
        "name='" + name + '\'' +
        ", callsign='" + callsign + '\'' +
        ", country='" + country + '\'' +
        '}';
    }
  }
  // end::airportclass[]

}

