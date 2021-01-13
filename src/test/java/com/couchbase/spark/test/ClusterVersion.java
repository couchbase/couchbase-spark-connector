/*
 * Copyright (c) 2020 Couchbase, Inc.
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
package com.couchbase.spark.test;

import java.util.Objects;

public class ClusterVersion {
    private final int majorVersion;
    private final int minorVersion;
    private final int patchVersion;

    public ClusterVersion(int majorVersion, int minorVersion, int patchVersion) {
        this.majorVersion = majorVersion;
        this.minorVersion = minorVersion;
        this.patchVersion = patchVersion;
    }

    public int majorVersion() {
        return majorVersion;
    }

    public int minorVersion() {
        return minorVersion;
    }

    public int patchVersion() {
        return patchVersion;
    }

    /**
     * Pass "6.5.1" or "6.5.1-SNAPSHOT" get a ClusterVersion back.  Any "-SNAPSHOT" is discarded.
     */
    public static ClusterVersion parseString(String version) {
        String[] splits = version.split("\\.");
        int majorVersion = Integer.parseInt(splits[0]);
        int minorVersion = Integer.parseInt(splits[1]);
        int patchVersion = Integer.parseInt(splits[2].split("-")[0]);

        return new ClusterVersion(majorVersion, minorVersion, patchVersion);
    }

    public boolean isGreaterThan(ClusterVersion other) {
        if (other.majorVersion > majorVersion) return true;
        if (other.majorVersion < majorVersion) return false;
        if (other.minorVersion > minorVersion) return true;
        if (other.minorVersion < minorVersion) return false;
        if (other.patchVersion > patchVersion) return true;
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterVersion that = (ClusterVersion) o;
        return majorVersion == that.majorVersion &&
                minorVersion == that.minorVersion &&
                patchVersion == that.patchVersion;
    }

    @Override
    public String toString() {
        return majorVersion + "." + minorVersion + "." + patchVersion;
    }

    @Override
    public int hashCode() {
        return Objects.hash(majorVersion, minorVersion, patchVersion);
    }
}
