/*
 * Copyright (c) 2015 Couchbase, Inc.
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
package com.couchbase.spark.api.python

import com.couchbase.client.core.message.kv.MutationToken
import com.couchbase.client.java.document._
import com.couchbase.client.java.document.json.JsonArray
import com.couchbase.spark.connection.SubdocLookupResult

import java.io.OutputStream
import java.util.{ArrayList => JArrayList}

import net.razorvine.pickle._
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.reflect.ClassTag

class PicklerUtils extends Serializable {
  register()
  def pickler() = {
    register() 
    new Pickler()
  }

  def unpickler() = {
    register() 
    new Unpickler()
  }
  def register(){
    RawJsonDocumentPickler.register()
    MutationTokenPickler.register()
    Pickler.registerCustomPickler(classOf[SubdocLookupResult], SubdocLookupResultPickler)
  }
}

object BatchPickler {
  private val picklers = new PicklerUtils()
  private val batchSize = 1000 
  def pickle(in: Iterator[_]): Iterator[Array[Byte]] = {
    in.grouped(batchSize).map { b => picklers.pickler().dumps(b.toArray) }
  }
}

object BatchUnpickler {
  
  private val picklers = new PicklerUtils()

  def unpickle(in: Array[Byte]): Seq[Any] = {
    val unpickled = picklers.unpickler().loads(in)
    unpickled match {
      case array: Array[Any] => array.toSeq
      case _ => unpickled.asInstanceOf[JArrayList[_]].asScala
    }
  }
}


/**
 * Base pickler class for serialization / deserialization between
 * python and java using Pickle.
 */
abstract class BasePickler[T: ClassTag] 
  extends IObjectPickler with IObjectConstructor {
  
  private val cls = implicitly[ClassTag[T]].runtimeClass
  
  def pyModule: String
  
  def pyType: String
  
  def pyTypePath = s"$pyModule\n$pyType\n"

  def register(): Unit = {
    Pickler.registerCustomPickler(cls, this)
    Unpickler.registerConstructor(pyModule, pyType, this)
  }

  /**
   * Java to python conversion, calls the registered
   * python function with arguments implemented in getPyArgs.
   */ 
  def pickle(o: Any, out: OutputStream, pickler: Pickler): Unit = {
    // push self.find_class(modname, name) 
    out.write(Opcodes.GLOBAL) 
    out.write(pyTypePath.getBytes()) 
    // push special markobject on stack 
    out.write(Opcodes.MARK) 
    getPyArgs(o.asInstanceOf[T]).foreach(pickler.save(_))
    // build tuple from topmost stack items 
    out.write(Opcodes.TUPLE)
    // apply callable to argtuple, both on stack 
    out.write(Opcodes.REDUCE) 
  }
  
  /**
   * Array of Any to call the python constructor with.
   * See https://github.com/irmen/Pyrolite for type mapping
   * from java to python
   */
  def getPyArgs(obj: T): Array[Any]
  
  /**
   * Construct scala object with array of java objects
   * from __reduce__ output on python
   */
  def construct(args: Array[AnyRef]): AnyRef
}

/**
 * RawJsonDocument serialization / deserialization
 */
object RawJsonDocumentPickler extends BasePickler[RawJsonDocument] {

  def pyModule = "pyspark_couchbase.types"
  def pyType = "RawJsonDocument"

  def getPyArgs(obj: RawJsonDocument): Array[Any] = {
    Array(obj.id, obj.content, obj.cas, obj.expiry, obj.mutationToken)
  }
  
  def construct(args: Array[AnyRef]): RawJsonDocument= {
    val id = args(0).asInstanceOf[String]
    val content = args(1).asInstanceOf[String]
    val cas = args(2).asInstanceOf[Number].longValue
    val expiry = args(3).asInstanceOf[Int]
    val mt = args(4).asInstanceOf[MutationToken]
    RawJsonDocument.create(id, expiry, content, cas, mt)
  }
}

/**
 * MutationToken serialization / deserialization
 */
object MutationTokenPickler extends BasePickler[MutationToken] {

  def pyModule = "pyspark_couchbase.types"
  def pyType = "MutationToken"

  def getPyArgs(obj: MutationToken): Array[Any] = {
    Array(obj.vbucketID, obj.vbucketUUID, obj.sequenceNumber, obj.bucket)
  }
  
  def construct(args: Array[AnyRef]): MutationToken = {
    val vbucketID = args(0).asInstanceOf[Number].longValue
    val vbucketUUID = args(1).asInstanceOf[Number].longValue
    val sequenceNumber = args(2).asInstanceOf[Number].longValue
    val bucket = args(3).asInstanceOf[String]
    new MutationToken(vbucketID, vbucketUUID, sequenceNumber, bucket)
  }
}

/**
 * SubdocLookupResult serialization 
 * Only Java to python is implemented for SubdocLookupResult
 */
object SubdocLookupResultPickler extends IObjectPickler {
  def pickle(o: Any, out: OutputStream, pickler: Pickler): Unit = {
    out.write(Opcodes.GLOBAL)
    out.write(pyTypePath.getBytes())
    out.write(Opcodes.MARK)
    val result = o.asInstanceOf[SubdocLookupResult]
    pickler.save(result.id)
    
    val (keys, vals) = result.content.toSeq.unzip
    val serializedVals = vals map {
      case a: Any => a.toString
      case _ => null // Converted to None on python side
    }
    pickler.save(seqAsJavaList(keys))
    pickler.save(seqAsJavaList(serializedVals)) 
    pickler.save(result.cas) 
    pickler.save(mapAsJavaMap(result.exists))
    out.write(Opcodes.TUPLE)
    out.write(Opcodes.REDUCE)
  }

  def pyTypePath = "pyspark_couchbase.types\n_create_subdoclookup_result\n"
}
