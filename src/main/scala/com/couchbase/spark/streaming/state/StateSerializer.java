package com.couchbase.spark.streaming.state;

public interface StateSerializer {
    void dump(ConnectorState connectorState);

    void dump(ConnectorState connectorState, short partition);

    ConnectorState load(ConnectorState connectorState);

    StreamState load(ConnectorState connectorState, short partition);
}