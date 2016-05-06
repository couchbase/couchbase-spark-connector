package com.couchbase.spark.streaming.state;

import com.couchbase.client.core.message.kv.MutationToken;

public class StreamState {
    private final MutationToken token;

    public StreamState(MutationToken token) {
        this.token = token;
    }

    public StreamState(short partition, long vbucketUUID, long sequenceNumber) {
        this.token = new MutationToken(partition, vbucketUUID, sequenceNumber, null);
    }

    /**
     * A unique identifier that is generated that is assigned to each VBucket.
     * This number is generated on an unclean shutdown or when a VBucket becomes
     * active.
     *
     * @return the stream vbucketUUID.
     */
    public long vbucketUUID() {
        return token.vbucketUUID();
    }

    /**
     * Specified the last by sequence number that has been seen by the consumer.
     *
     * @return the stream last sequence number.
     */
    public long sequenceNumber() {
        return token.sequenceNumber();
    }

    /**
     * The partition number (vBucket), to which this state belongs.
     *
     * @return the stream partition number.
     */
    public short partition() {
        return (short) token.vbucketID();
    }

    @Override
    public String toString() {
        return token.toString();
    }
}