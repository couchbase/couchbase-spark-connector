/*
 * Copyright (c) 2021 Couchbase, Inc.
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
package examples

import com.couchbase.spark.kv.Get
import org.apache.spark.sql.SparkSession

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

object EncryptionExample {

  val CERT =
    "-----BEGIN CERTIFICATE-----\nMIIDAjCCAeqgAwIBAgIIFpvHWOT/rckwDQYJKoZIhvcNAQELBQAwJDEiMCAGA1UE\nAxMZQ291Y2hiYXNlIFNlcnZlciA0ODk0MDYzZjAeFw0xMzAxMDEwMDAwMDBaFw00\nOTEyMzEyMzU5NTlaMCQxIjAgBgNVBAMTGUNvdWNoYmFzZSBTZXJ2ZXIgNDg5NDA2\nM2YwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQC/xDaxCYZHYJTpku9j\nGVi8Exf6xM8IA1WD8WEZrLyXE1SfXIapPr8HuEGo14Ym+E23fDKUMUs93PP+ZWM7\nzgd+W0HWckYGYvTjplKo3mOHhwwo+KyJObiuzU5xkUvAggrT7qyw82kixGBeF89L\nGwHfbpJ8daUCkeT2KrC+Yb5tIinQpOrSXX5YdTc/fn4LMDW5JjW3Ll7qB8UnFUK1\nDdLZVUlVK0GqirEZNat22gxoWvJSRDvtnv4WnVG7r7W/TTf7Rd6Z91HyHD7ZDnof\nESvrq9X2Zxj1c2x7gUg6X5Z771rWoBxOzVCmpb/RUq/xr/f+HKA3eLIUjnvY4dAj\nhZGJAgMBAAGjODA2MA4GA1UdDwEB/wQEAwICpDATBgNVHSUEDDAKBggrBgEFBQcD\nATAPBgNVHRMBAf8EBTADAQH/MA0GCSqGSIb3DQEBCwUAA4IBAQCX5MBLC0SC6nGz\n5Fffq+IUMd39v12IxHsfY7Rao2o4liHN1n0NPY+WxOwYYMPHq/c73G9ld0jdi+Cc\nroeEvp10dWmdseagvFv7mUL+XeAVqpME1WShKS2U4qmMCw26szFAQJz0W7KpEEKV\nUN3qBDHenISzkelHxydMHuKhJ05NeaESXCERpnaWxSu2aa+EKiegKAV0l0paFi34\nzJ5uNnHhVAWfHaJCC+4xDrPrwjBcKWv2Eszkm/aZPuIE3H2/AlEyB3zB5wrTYaCi\necoUEby/b9F29zGx4zxStfLGVh8pfF7Gu/gLzIonSgSOE9fAxWyiEvfwCEMiEn5i\nLVxKCgUd\n-----END CERTIFICATE-----"

  def main(args: Array[String]): Unit = {
    val certPath = "cert.pem"
    Files.write(Paths.get(certPath), CERT.getBytes(StandardCharsets.UTF_8))

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Encryption RDD Example")
      .config("spark.couchbase.connectionString", "127.0.0.1")
      .config("spark.couchbase.username", "Administrator")
      .config("spark.couchbase.password", "password")
      .config("spark.couchbase.implicitBucket", "travel-sample")
      // Enable TLS and provide path to cert
      .config("spark.couchbase.security.enableTls", "true")
      .config("spark.couchbase.security.trustCertificate", certPath)
      .getOrCreate()

    import com.couchbase.spark._

    spark.sparkContext
      .couchbaseGet(Seq(Get("airline_10")))
      .collect()
      .foreach(println)
  }
}
