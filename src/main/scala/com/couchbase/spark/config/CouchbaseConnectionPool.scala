package com.couchbase.spark.config

import org.apache.spark.internal.Logging

import java.util.concurrent.ConcurrentHashMap

class CouchbaseConnectionPool extends Serializable with Logging {

  val connectionPool = new ConcurrentHashMap[Int,CouchbaseConnection]()

  def getConnection(cfg: CouchbaseConfig): CouchbaseConnection = {
    if (!connectionPool.containsKey(cfg.getKey)) {
      this.synchronized {
        if (!connectionPool.containsKey(cfg.getKey)) {
          val connection = CouchbaseConnection(cfg)
          connectionPool.put(cfg.getKey, connection)
        }
      }
    }
    connectionPool.get(cfg.getKey)
  }

  def closeConnection(cfg: CouchbaseConfig): Unit = {
    if (connectionPool.contains(cfg.getKey)){
      this.synchronized{
        if (connectionPool.contains(cfg.getKey)) {
          connectionPool.remove(cfg.getKey)
        }
      }
    }
  }

}

object CouchbaseConnectionPool{
  var couchBaseConnectionPool: Option[CouchbaseConnectionPool] = None
  def apply() = {
    if (!couchBaseConnectionPool.isDefined){
      this.synchronized{
        if (!couchBaseConnectionPool.isDefined){
          couchBaseConnectionPool = Some(new CouchbaseConnectionPool)
        }
      }
    }
    couchBaseConnectionPool.get
  }
}
