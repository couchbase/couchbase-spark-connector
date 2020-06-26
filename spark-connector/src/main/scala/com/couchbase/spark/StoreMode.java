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
package com.couchbase.spark;

/**
 * The {@link StoreMode} that is used to save the RDD.
 */
public enum StoreMode {

    /**
     * Store the document, insert if it does not exist and override if it does.
     */
    UPSERT,

    /**
     * Try to insert the document, but fail if it does exist.
     */
    INSERT_AND_FAIL,

    /**
     * Try to insert the document and ignore errors if they do exist (the document on the
     * server will be preserved).
     */
    INSERT_AND_IGNORE,

    /**
     * Try to replace the document, and fail if it does exist.
     */
    REPLACE_AND_FAIL,

    /**
     * Try to replace the document and ignore errors if it doesn't exist.
     */
    REPLACE_AND_IGNORE

}
