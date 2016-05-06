package com.couchbase.spark.streaming.state;


public class NoopStateSerializer implements StateSerializer {

    @Override
    public void dump(ConnectorState connectorState) {

    }

    @Override
    public void dump(ConnectorState connectorState, short partition) {

    }

    @Override
    public ConnectorState load(ConnectorState connectorState) {
        return null;
    }

    @Override
    public StreamState load(ConnectorState connectorState, short partition) {
        return null;
    }
}
