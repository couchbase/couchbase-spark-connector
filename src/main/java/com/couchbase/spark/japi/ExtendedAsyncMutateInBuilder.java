/*
 * Copyright (c) 2017 Couchbase, Inc.
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

import com.couchbase.client.core.ClusterFacade;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.subdoc.AsyncMutateInBuilder;
import com.couchbase.client.java.subdoc.MutationSpec;
import com.couchbase.client.java.transcoder.subdoc.FragmentTranscoder;

/**
 * Extension class to the {@link AsyncMutateInBuilder} to expose the mutation specs.
 *
 * @author Michael Nitschinger
 * @since 2.1.1
 */
public class ExtendedAsyncMutateInBuilder extends AsyncMutateInBuilder {

    public ExtendedAsyncMutateInBuilder(ClusterFacade core, String bucketName, CouchbaseEnvironment environment, FragmentTranscoder transcoder, String docId) {
        super(core, bucketName, environment, transcoder, docId);
    }

    public void addMutationSpec(final MutationSpec spec) {
        this.mutationSpecs.add(spec);
    }


}
