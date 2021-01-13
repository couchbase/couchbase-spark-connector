/*
 * Copyright (c) 2019 Couchbase, Inc.
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
package com.couchbase.spark.test;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ ElementType.ANNOTATION_TYPE, ElementType.METHOD, ElementType.TYPE })
@Retention(RetentionPolicy.RUNTIME)
public @interface IgnoreWhen {
  ClusterType[] clusterTypes() default {};

  Capabilities[] missesCapabilities() default {};

  Capabilities[] hasCapabilities() default {};

  int nodesLessThan() default 0;

  int nodesGreaterThan() default Integer.MAX_VALUE;

  int replicasLessThan() default 0;

  int replicasGreaterThan() default Integer.MAX_VALUE;

  /**
   * Do not run a test when the cluster version is equal to this field.
   * <p>
   * The field should be given as a raw String e.g. "6.5.1" (due to Java limitations it's not possible to specify
   * a {@link ClusterVersion} directly.
   * <p>
   * It is strongly recommended that one of the alternatives above is used.  All cluster features _should_ be exposed
   * properly through, for instance, {@link #hasCapabilities()}.  This field should only be used when those avenues
   * have failed - an example would be a cluster bug that only happens in a specific version.
   */
  String clusterVersionEquals() default "";
}
