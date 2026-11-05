/*
 * Copyright (c) 2026 Couchbase, Inc.
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
package com.couchbase.spark.util

import java.util.regex.Pattern

/** Removes secrets from values before they are logged or otherwise turned into strings. */
private[spark] object Redaction {

  val Redacted = "*****"

  private val SensitiveKey = Pattern.compile("(?i).*(password|secret|token).*")

  def isSensitiveKey(key: String): Boolean = SensitiveKey.matcher(key).matches()

  /** Redacts the password, and any parameter whose name looks sensitive, from a dynamic
    * connection identifier such as "couchbase://user:password@host?spark.ssl.keyStorePassword=x".
    *
    * The split into user, hosts and parameters follows the SDK's ConnectionString: the user part
    * runs to the first '@', and the parameters start at the first '?' after that.
    *
    * Static connection identifiers (plain names such as "test") contain no secrets and are
    * returned unchanged.
    */
  def connectionIdentifier(identifier: String): String = {
    if (identifier == null) return null
    val schemeEnd = identifier.indexOf("://")
    if (schemeEnd < 0) return identifier

    val afterScheme = schemeEnd + 3
    val out         = new StringBuilder(identifier.substring(0, afterScheme))

    val at = identifier.indexOf('@', afterScheme)
    val rest = if (at >= 0) {
      val user  = identifier.substring(afterScheme, at)
      val colon = user.indexOf(':')
      if (colon >= 0) out.append(user.substring(0, colon + 1)).append(Redacted)
      else out.append(user)
      out.append('@')
      identifier.substring(at + 1)
    } else {
      identifier.substring(afterScheme)
    }

    val question = rest.indexOf('?')
    if (question < 0) {
      out.append(rest)
    } else {
      out.append(rest.substring(0, question + 1))
      out.append(
        rest
          .substring(question + 1)
          .split("&", -1)
          .map(param => {
            val eq = param.indexOf('=')
            if (eq > 0 && isSensitiveKey(param.substring(0, eq))) {
              param.substring(0, eq + 1) + Redacted
            } else {
              param
            }
          })
          .mkString("&")
      )
    }

    out.toString
  }

  def connectionIdentifier(identifier: Option[String]): Option[String] =
    identifier.map(i => connectionIdentifier(i))

  def properties(props: Seq[(String, String)]): Seq[(String, String)] =
    props.map { case (k, v) => if (isSensitiveKey(k)) (k, Redacted) else (k, v) }
}
