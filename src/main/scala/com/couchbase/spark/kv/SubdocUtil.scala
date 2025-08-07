/*
 * Copyright (c) 2025 Couchbase, Inc.
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
package com.couchbase.spark.kv

import com.couchbase.client.scala.codec.JsonSerializer
import com.couchbase.client.scala.json.{JsonArray, JsonObject}
import com.couchbase.client.scala.kv.{MutateInSpec, MutateInSpecStandard}
import org.apache.spark.sql.types.StructType
// These implicit imports should not be required (the compiler finds them fine), but IntelliJ's Scala plugin is unhappy without them
import com.couchbase.client.scala.codec.JsonSerializer.StringConvert
import com.couchbase.client.scala.codec.JsonSerializer.JsonObjectConvert
import com.couchbase.client.scala.codec.JsonSerializer.JsonArrayConvert
import com.couchbase.client.scala.codec.JsonSerializer.LongConvert
import com.couchbase.client.scala.codec.JsonSerializer.IntConvert
import com.couchbase.client.scala.codec.JsonSerializer.DoubleConvert
import com.couchbase.client.scala.codec.JsonSerializer.BooleanConvert

private[couchbase] object SubdocUtil {

  sealed abstract trait SubdocOperation {
    def name: String
  }

  case object Insert extends SubdocOperation { val name = "insert" }
  case object Upsert extends SubdocOperation { val name = "upsert" }
  case object Remove extends SubdocOperation { val name = "remove" }
  case object Replace extends SubdocOperation { val name = "replace" }
  case object ArrayAppend extends SubdocOperation { val name = "arrayappend" }
  case object ArrayPrepend extends SubdocOperation { val name = "arrayprepend" }
  case object ArrayInsert extends SubdocOperation { val name = "arrayinsert" }
  case object ArrayAddUnique extends SubdocOperation { val name = "arrayaddunique" }
  case object Increment extends SubdocOperation { val name = "increment" }
  case object Decrement extends SubdocOperation { val name = "decrement" }

  private val MaxSpecs = 16

  private val SupportedOps: Set[SubdocOperation] = Set(
    Insert,
    Upsert,
    Remove,
    Replace,
    ArrayAppend,
    ArrayPrepend,
    ArrayInsert,
    ArrayAddUnique,
    Increment,
    Decrement
  )

  private def parseOperation(opStr: String): SubdocOperation = {
    opStr.toLowerCase match {
      case "insert" => Insert
      case "upsert" => Upsert
      case "remove" => Remove
      case "replace" => Replace
      case "arrayappend" => ArrayAppend
      case "arrayprepend" => ArrayPrepend
      case "arrayinsert" => ArrayInsert
      case "arrayaddunique" => ArrayAddUnique
      case "increment" => Increment
      case "decrement" => Decrement
      case _ => throw new IllegalArgumentException(s"Unsupported sub-document operation '$opStr'")
    }
  }

  private def buildArraySpec(op: SubdocOperation, path: String, v: Any): MutateInSpec = {

    def build[A](xs: Seq[A])(implicit s: JsonSerializer[A]): MutateInSpec =
      op match {
        case ArrayAppend  => MutateInSpec.arrayAppend(path, xs)
        case ArrayPrepend => MutateInSpec.arrayPrepend(path, xs)
        case ArrayInsert  => MutateInSpec.arrayInsert(path, xs)
        case _ => throw new IllegalArgumentException(s"Operation ${op.name} is not supported for array operations")
      }

    v match {
      case jo: JsonObject => build[JsonObject](Seq(jo))
      case s: String      => build[String](Seq(s))
      case l: Long        => build[Long](Seq(l))
      case i: Int         => build[Long](Seq(i.toLong))
      case d: Double      => build[Double](Seq(d))
      case f: Float       => build[Double](Seq(f.toDouble))
      case b: Boolean     => build[Boolean](Seq(b))
      case null           => build[String](Seq(null))

      // There are two valid things to do for Spark arrays: treat them as one atomic thing that the user wants to handle,
      // or treat them as their component elements.
      // If we are doing an arrayInsert operation into existing array ["A", "D"] and we have been passed ["B", "C"],
      // both are valid:
      // 1. ["A", ["B", "C"], "D"]
      // 2. ["A", "B", "C", "D"]
      // We support (2) as it allows (1) also, if the user passes [["B", "C"]]
      case arr: JsonArray =>
        if (arr.isEmpty)
          throw new IllegalArgumentException(
            s"$op on '$path' received an empty array, which is not supported"
          )

        // Note the SDK does not support heterogeneous collections for these array operations.
        arr.get(0) match {
          case _: JsonObject =>
            val xs = arr.toSeq
            if (!xs.forall(_.isInstanceOf[JsonObject]))
              throw new IllegalArgumentException(
                s"All elements in array for $op on '$path' must be JsonObject"
              )
            build[JsonObject](xs.map(_.asInstanceOf[JsonObject]))
          case _: JsonArray =>
            val xs = arr.toSeq
            if (!xs.forall(_.isInstanceOf[JsonArray]))
              throw new IllegalArgumentException(
                s"All elements in array for $op on '$path' must be JsonArray"
              )
            build[JsonArray](xs.map(_.asInstanceOf[JsonArray]))
          case _: String =>
            val xs = arr.toSeq
            if (!xs.forall(_.isInstanceOf[String]))
              throw new IllegalArgumentException(
                s"All elements in array for $op on '$path' must be String"
              )
            build[String](xs.map(_.asInstanceOf[String]))
          case _: java.lang.Long | _: Int =>
            val xs = arr.toSeq
            if (!xs.forall(e => e.isInstanceOf[java.lang.Long] || e.isInstanceOf[Int]))
              throw new IllegalArgumentException(
                s"All elements in array for $op on '$path' must be long integers"
              )
            build[Long](xs.map { case n: Number => n.longValue })
          case _: java.lang.Double | _: Float =>
            val xs = arr.toSeq
            if (!xs.forall(e => e.isInstanceOf[java.lang.Double] || e.isInstanceOf[Float]))
              throw new IllegalArgumentException(
                s"All elements in array for $op on '$path' must be floating-point numbers"
              )
            build[Double](xs.map { case n: Number => n.doubleValue })
          case _: java.lang.Boolean =>
            val xs = arr.toSeq
            if (!xs.forall(_.isInstanceOf[java.lang.Boolean]))
              throw new IllegalArgumentException(
                s"All elements in array for $op on '$path' must be Boolean"
              )
            build[Boolean](xs.map { case b: java.lang.Boolean => b.booleanValue })
          case other =>
            throw new IllegalArgumentException(
              s"Unsupported element type in array for $op on '$path': ${other.getClass}"
            )
        }

      case other =>
        throw new IllegalArgumentException(
          s"Unsupported value for $op on '$path': ${other.getClass}"
        )
    }
  }

  def validate(schema: StructType, idFieldName: String, casFieldNameOpt: Option[String]): Unit = {
    val nonSpecialCols =
      schema.fields.filter(f => f.name != idFieldName && !casFieldNameOpt.contains(f.name))

    if (nonSpecialCols.length > MaxSpecs) {
      throw new IllegalArgumentException(
        s"Sub-document operations support maximum $MaxSpecs specs but ${nonSpecialCols.length} columns provided"
      )
    }

    nonSpecialCols.foreach { field =>
      val colName  = field.name
      // This indexOf(":"), and the use of ":" as the special char, are safe.  References:
      // [Subdoc spec]: "The path specifies a location within the document upon which the command should execute. The
      // path assumes dots (.) to describe the hierarchy which should point to the target field. The syntax of the path
      // is intended to conform to the N1QL syntax for specifying a location within a document"
      // https://docs.couchbase.com/server/current/n1ql/n1ql-language-reference/select-syntax.html#result-expr
      // https://docs.couchbase.com/server/current/n1ql/n1ql-language-reference/select-syntax.html#path
      // https://docs.couchbase.com/server/current/n1ql/n1ql-language-reference/identifiers.html
      val splitIdx = colName.indexOf(":")
      val opStr    = (if (splitIdx == -1) "upsert" else colName.substring(0, splitIdx)).toLowerCase
      val op = parseOperation(opStr)
      if (!SupportedOps.contains(op)) {
        throw new IllegalArgumentException(
          s"Unsupported sub-document operation '$opStr' in column header '$colName'"
        )
      }
    }
  }

  def buildSubdocSpecs(decoded: JsonObject): (Seq[MutateInSpec], String) = {
    val fieldNames: Seq[String] = decoded.names.toSeq
    val debugInfos              = scala.collection.mutable.ArrayBuffer[String]()

    val specs = fieldNames.flatMap { fieldName =>
      val value = decoded.get(fieldName)
      val splitIdx = fieldName.indexOf(":")
      val (opRaw, path) = if (splitIdx == -1) {
        (Upsert, fieldName)
      } else {
        (parseOperation(fieldName.substring(0, splitIdx).toLowerCase), fieldName.substring(splitIdx + 1))
      }

      debugInfos += s"${opRaw.name}@$path"

      val spec: MutateInSpec = opRaw match {
        case Insert =>
          value match {
            case v: JsonObject => MutateInSpec.insert(path, v)
            case v: JsonArray  => MutateInSpec.insert(path, v)
            case v: String     => MutateInSpec.insert(path, v)
            case v: Long       => MutateInSpec.insert(path, v)
            case v: Integer    => MutateInSpec.insert(path, v.toLong)
            case v: Double     => MutateInSpec.insert(path, v)
            case v: Float      => MutateInSpec.insert(path, v.toDouble)
            case v: Boolean    => MutateInSpec.insert(path, v)
            case null          => MutateInSpec.insert(path, null.asInstanceOf[String])
            case v =>
              throw new IllegalArgumentException(
                s"Unsupported value type for insert operation on path '$path': ${v.getClass.getSimpleName} ${v}"
              )
          }
        case Upsert =>
          value match {
            case v: JsonObject => MutateInSpec.upsert(path, v)
            case v: JsonArray  => MutateInSpec.upsert(path, v)
            case v: String     => MutateInSpec.upsert(path, v)
            case v: Long       => MutateInSpec.upsert(path, v)
            case v: Integer    => MutateInSpec.upsert(path, v.toLong)
            case v: Double     => MutateInSpec.upsert(path, v)
            case v: Float      => MutateInSpec.upsert(path, v.toDouble)
            case v: Boolean    => MutateInSpec.upsert(path, v)
            case null          => MutateInSpec.upsert(path, null.asInstanceOf[String])
            case v =>
              throw new IllegalArgumentException(
                s"Unsupported value type for upsert operation on path '$path': ${v.getClass.getSimpleName} ${v}"
              )
          }
        case Remove =>
          // Note: Spark removes null fields before they reach the connector, so even though we don't use the DF value, cannot use null for it
          MutateInSpec.remove(path)
        case Replace =>
          value match {
            case v: JsonObject => MutateInSpec.replace(path, v)
            case v: JsonArray  => MutateInSpec.replace(path, v)
            case v: String     => MutateInSpec.replace(path, v)
            case v: Long       => MutateInSpec.replace(path, v)
            case v: Int        => MutateInSpec.replace(path, v.toLong)
            case v: Double     => MutateInSpec.replace(path, v)
            case v: Float      => MutateInSpec.replace(path, v.toDouble)
            case v: Boolean    => MutateInSpec.replace(path, v)
            case null          => MutateInSpec.replace(path, null.asInstanceOf[String])
            case v =>
              throw new IllegalArgumentException(
                s"Unsupported value type for replace operation on path '$path': ${v.getClass.getSimpleName} ${v}"
              )
          }
        case ArrayAppend | ArrayPrepend | ArrayInsert =>
          buildArraySpec(opRaw, path, value)
        case ArrayAddUnique =>
          value match {
            case v: JsonObject => MutateInSpec.arrayAddUnique(path, v)
            case v: JsonArray  => MutateInSpec.arrayAddUnique(path, v)
            case v: String     => MutateInSpec.arrayAddUnique(path, v)
            case v: Long       => MutateInSpec.arrayAddUnique(path, v)
            case v: Int        => MutateInSpec.arrayAddUnique(path, v)
            case v: Double     => MutateInSpec.arrayAddUnique(path, v)
            case v: Float      => MutateInSpec.arrayAddUnique(path, v.toDouble)
            case v: Boolean    => MutateInSpec.arrayAddUnique(path, v)
            case null          => MutateInSpec.arrayAddUnique(path, null.asInstanceOf[String])
            case v =>
              throw new IllegalArgumentException(
                s"Unsupported value type for arrayAddUnique operation on path '$path': ${v.getClass.getSimpleName} ${v}"
              )
          }
        case Increment =>
          value match {
            case v: Long   => MutateInSpec.increment(path, v)
            case v: Int    => MutateInSpec.increment(path, v.toLong)
            case v: Double => MutateInSpec.increment(path, v.toLong)
            case v: Float  => MutateInSpec.increment(path, v.toLong)
            case v =>
              throw new IllegalArgumentException(
                s"Increment operation requires numeric value on path '$path', got: ${v.getClass.getSimpleName} ${v}"
              )
          }
        case Decrement =>
          value match {
            case v: Long   => MutateInSpec.decrement(path, v)
            case v: Int    => MutateInSpec.decrement(path, v.toLong)
            case v: Double => MutateInSpec.decrement(path, v.toLong)
            case v: Float  => MutateInSpec.decrement(path, v.toLong)
            case v =>
              throw new IllegalArgumentException(
                s"Decrement operation requires numeric value on path '$path', got: ${v.getClass.getSimpleName} ${v}"
              )
          }
        case _ =>
          throw new IllegalArgumentException(
            s"Unsupported sub-document operation '$opRaw' for path '$path'"
          )
      }
      Some(spec)
    }

    (specs, debugInfos.mkString(", "))
  }
}
