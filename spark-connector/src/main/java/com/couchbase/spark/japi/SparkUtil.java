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
package com.couchbase.spark.japi;

import scala.collection.Seq;
import scala.reflect.ClassTag;
import java.util.List;

public class SparkUtil {

    public static <T> ClassTag<T> classTag(Class<T> source) {
        return scala.reflect.ClassTag$.MODULE$.apply(source);
    }

    public static <T> Seq<T> listToSeq(List<T> source) {
        return scala.collection.JavaConversions.asScalaBuffer(source).seq();
    }
}
