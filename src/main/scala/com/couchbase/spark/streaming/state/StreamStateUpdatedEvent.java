package com.couchbase.spark.streaming.state;

public class StreamStateUpdatedEvent {
    private final ConnectorState connectorState;
    private final short partition;

    public StreamStateUpdatedEvent(ConnectorState connectorState, short partition) {
        this.connectorState = connectorState;
        this.partition = partition;
    }

    public ConnectorState connectorState() {
        return connectorState;
    }

    public short partition() {
        return partition;
    }
}