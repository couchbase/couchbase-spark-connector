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
