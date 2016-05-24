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
package com.couchbase.spark.streaming;


import com.couchbase.client.core.ClusterFacade;
import com.couchbase.client.core.endpoint.dcp.DCPConnection;
import com.couchbase.client.core.message.ResponseStatus;
import com.couchbase.client.core.message.dcp.DCPMessage;
import com.couchbase.client.core.message.dcp.DCPRequest;
import com.couchbase.client.core.message.dcp.OpenConnectionRequest;
import com.couchbase.client.core.message.dcp.OpenConnectionResponse;
import com.couchbase.client.core.message.kv.MutationToken;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.spark.streaming.state.ConnectorState;
import com.couchbase.spark.streaming.state.StateSerializer;
import com.couchbase.spark.streaming.state.StreamState;
import com.couchbase.spark.streaming.state.StreamStateUpdatedEvent;
import rx.Observable;
import rx.functions.Action1;
import rx.functions.Action2;
import rx.functions.Func0;
import rx.functions.Func1;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * {@link CouchbaseReader} is in charge of accepting events from Couchbase.
 *
 * @author Sergey Avseyev
 * @author Michael Nitschinger
 */
public class CouchbaseReader {

    private static final int BUFFER_SIZE = 16384;

    private final ClusterFacade core;
    private final String bucket;
    private final StateSerializer stateSerializer;
    private final String connectionName;
    private final CouchbaseEnvironment environment;
    private DCPConnection connection;

    /**
     * Creates a new {@link CouchbaseReader}.
     *
     * @param bucket            bucket name to override
     * @param core              the core reference.
     * @param environment       the environment object, which carries settings.
     * @param stateSerializer   the object to serialize the state of DCP streams.
     */
    public CouchbaseReader(final String bucket,
                           final ClusterFacade core, final CouchbaseEnvironment environment,
                           final StateSerializer stateSerializer) {
        this.core = core;
        this.bucket = bucket;
        this.stateSerializer = stateSerializer;
        this.connectionName = "CouchbaseSpark(" + this.hashCode() + ")";
        this.environment = environment;
    }

    /**
     * Performs connection with default timeout.
     */
    public void connect() {
        connect(environment.connectTimeout(), TimeUnit.SECONDS);
    }

    /**
     * Performs connection with arbitrary timeout
     *
     * @param timeout  the custom timeout.
     * @param timeUnit the unit for the timeout.
     */
    private void connect(final long timeout, final TimeUnit timeUnit) {
        OpenConnectionResponse response =
            core.<OpenConnectionResponse>send(new OpenConnectionRequest(connectionName, bucket))
            .timeout(timeout, timeUnit)
            .toBlocking()
            .single();
        this.connection = response.connection();
    }


    /**
     * Returns current state of the cluster.
     *
     * @return and object, which contains current sequence number for each partition on the cluster.
     */
    private ConnectorState _currentState() {
        return connection.getCurrentState()
            .collect(new Func0<ConnectorState>() {
                @Override
                public ConnectorState call() {
                    return new ConnectorState();
                }
            }, new Action2<ConnectorState, MutationToken>() {
                @Override
                public void call(ConnectorState connectorState, MutationToken token) {
                    connectorState.put(new StreamState(token));
                }
            })
            .toBlocking()
            .single();
    }

    public ConnectorState currentState(int... partitions) {
        ConnectorState currentState = _currentState();
        if (partitions.length == 0) {
            return currentState;
        }
        ConnectorState state = new ConnectorState();
        for (int partition : partitions) {
            state.put(currentState.get((short) partition));
        }
        return state;
    }

    public ConnectorState startState(int... partitions) {
        return overrideSequenceNumber(currentState(partitions), 0);
    }

    public ConnectorState endState(int... partitions) {
        return overrideSequenceNumber(currentState(partitions), 0xffffffff);
    }

    private ConnectorState overrideSequenceNumber(ConnectorState connectorState, long sequenceNumber) {
        ConnectorState state = new ConnectorState();
        for (StreamState streamState : connectorState) {
            state.put(new StreamState(streamState.partition(), streamState.vbucketUUID(), sequenceNumber));
        }
        return state;
    }

    /**
     * Executes worker reading loop, which relays events from Couchbase to Kafka.
     *
     * @param fromState initial state for the streams
     * @param toState   target state for the streams
     */
    public Observable<DCPRequest> run(final ConnectorState fromState, final ConnectorState toState) {
        if (!Arrays.equals(fromState.partitions(), toState.partitions())) {
            throw new IllegalArgumentException("partitions in FROM state do not match partitions in TO state");
        }

        final ConnectorState connectorState = fromState.clone();
        connectorState.updates().subscribe(
            new Action1<StreamStateUpdatedEvent>() {
                @Override
                public void call(StreamStateUpdatedEvent event) {
                    stateSerializer.dump(event.connectorState(), event.partition());
                }
            });

        return Observable.from(fromState)
            .flatMap(new Func1<StreamState, Observable<ResponseStatus>>() {
                @Override
                public Observable<ResponseStatus> call(StreamState begin) {
                    StreamState end = toState.get(begin.partition());
                    return connection.addStream(begin.partition(), begin.vbucketUUID(),
                        begin.sequenceNumber(), end.sequenceNumber(),
                        begin.sequenceNumber(), end.sequenceNumber());
                }
            })
            .toList()
            .flatMap(new Func1<List<ResponseStatus>, Observable<DCPRequest>>() {
                @Override
                public Observable<DCPRequest> call(List<ResponseStatus> statuses) {
                    return connection.subject();
                }
            })
            .onBackpressureBuffer(BUFFER_SIZE);
    }

    public <T extends DCPMessage> void consumed(T request) {
        connection.consumed(request);
    }
}