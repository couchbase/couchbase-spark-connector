package com.couchbase.spark.streaming.state;

import rx.Observable;
import rx.subjects.PublishSubject;
import rx.subjects.Subject;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Implements state of the DStream instance.
 */
public class ConnectorState implements Iterable<StreamState> {
    private final Map<Short, StreamState> streams;
    private final Subject<StreamStateUpdatedEvent, StreamStateUpdatedEvent> updates =
        PublishSubject.<StreamStateUpdatedEvent>create().toSerialized();

    public ConnectorState() {
        this.streams = new HashMap<Short, StreamState>(1024);
    }

    @Override
    public Iterator<StreamState> iterator() {
        return streams.values().iterator();
    }

    /**
     * Set/update the stream state
     *
     * @param streamState new state for stream
     */
    public void put(StreamState streamState) {
        streams.put(streamState.partition(), streamState);
    }

    /**
     * Returns the stream state.
     *
     * @param partition partition of the stream.
     * @return the state associated or null
     */
    public StreamState get(short partition) {
        return streams.get(partition);
    }

    public short[] partitions() {
        short[] partitions = new short[streams.size()];
        int i = 0;
        for (Short partition : streams.keySet()) {
            partitions[i++] = partition;
        }
        return partitions;
    }

    public ConnectorState clone() {
        ConnectorState newState = new ConnectorState();
        for (Map.Entry<Short, StreamState> entry : streams.entrySet()) {
            newState.streams.put(entry.getKey(), entry.getValue());
        }
        return newState;
    }

    public void update(short partition, long sequenceNumber) {
        StreamState state = streams.get(partition);
        streams.put(partition,
            new StreamState(partition, state.vbucketUUID(), Math.max(state.sequenceNumber(), sequenceNumber)));
        updates.onNext(new StreamStateUpdatedEvent(this, partition));
    }

    public Observable<StreamStateUpdatedEvent> updates() {
        return updates;
    }

    @Override
    public String toString() {
        return "ConnectorState{" +
            "streams=" + streams +
            '}';
    }
}