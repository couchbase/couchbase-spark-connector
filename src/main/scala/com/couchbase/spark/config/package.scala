package com.couchbase.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util.Map
import java.util.HashMap
import collection.JavaConverters._
import scala.language.implicitConversions

package object config {

  implicit def sparkConfToMap(conf: SparkConf): Map[String,String] = {
    val properties = new HashMap[String,String]()
    conf.getAll.map(kv => properties.put(kv._1,kv._2))
    properties
  }

  implicit def mapToSparkConf(properties: Map[String,String]): SparkConf  = {
    val sparkConf = new SparkConf()
    properties.asScala.map(kv => sparkConf.set(kv._1,kv._2))
    sparkConf
  }

  implicit def optionsToSparkConf(options: CaseInsensitiveStringMap): SparkConf  = {
    val sparkConf = new SparkConf()
    options.asScala.map(kv => sparkConf.set(kv._1,kv._2))
    sparkConf
  }
}
