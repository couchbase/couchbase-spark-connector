package com.couchbase.spark.japi;

import java.io.Serializable;

public class Airport implements Serializable {
    private String name;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}