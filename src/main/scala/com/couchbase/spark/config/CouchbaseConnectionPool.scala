package com.couchbase.spark.config

import java.util.concurrent.ConcurrentHashMap

class CouchbaseConnectionPool {

  var connectionPool = new ConcurrentHashMap[String,CouchbaseConnection]()

  def getConnection(cfg: CouchbaseConfig): CouchbaseConnection = {
    if (!connectionPool.contains(cfg.getKey)){
      this.synchronized{
        if (!connectionPool.contains(cfg.getKey)) {
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
  lazy val connectionPool = new CouchbaseConnectionPool
  def apply() = connectionPool
}
