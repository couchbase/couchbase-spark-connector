package com.couchbase.spark.rdd

import com.couchbase.client.core.config.NodeInfo
import com.couchbase.client.core.message.cluster.{GetClusterConfigRequest, GetClusterConfigResponse}
import com.couchbase.client.core.service.ServiceType
import com.couchbase.spark.connection.{CouchbaseConfig, CouchbaseConnection}
import org.apache.spark.Partition
import rx.lang.scala.JavaConversions.toScalaObservable

/**
  * Utility code shared by the RDDs
  */
object RDDSupport {
  /**
    * Find the hostnames of all Couchbase nodes running a particular service
    */
  def couchbaseNodesWithService(cbConfig: CouchbaseConfig,
                                bucketName: String,
                                serviceType: ServiceType): Seq[String] = {
    // Config comes from SparkContext which is usually bound to one bucket, so the typically
    // null bucketName here is ok.  If app has specified multiple buckets in SparkContext, it
    // will need to disambiguate by providing non-null bucket here
    val core = CouchbaseConnection().bucket(cbConfig, bucketName).core()
    import collection.JavaConverters._

    val req = new GetClusterConfigRequest()
    val config = toScalaObservable(core.send[GetClusterConfigResponse](req))
      .toBlocking
      .single

    val addressesWithQueryService: Seq[String] = config.config().bucketConfigs().asScala
      .flatMap(v => {
        val bucketConfig = v._2
        bucketConfig.nodes.asScala
          .filter(node => node.services().asScala.contains(serviceType))
      })
      .map(v => RDDSupport.extractNodeHostname(v))
      .toSeq
      .distinct

    addressesWithQueryService
  }

  /**
    * Extracts the preferred hostname from a QueryPartition
    */
  def getPreferredLocations(split: Partition): Seq[String] = {
    val p = split.asInstanceOf[QueryPartition]

    // If the user has co-located Spark worker services on Couchbase nodes, this will get the query
    // to run on a Spark worker running on a relevant Couchbase node, if possible
    val out = if (p.hostnames.nonEmpty) {
      p.hostnames
    } else {
      Nil
    }
    out
  }

  def extractNodeHostname(nodeInfo: NodeInfo): String =
    if (nodeInfo.useAlternateNetwork() != null) {
      nodeInfo.alternateAddresses().get(nodeInfo.useAlternateNetwork()).hostname()
    } else {
      nodeInfo.hostname()
    }

}
