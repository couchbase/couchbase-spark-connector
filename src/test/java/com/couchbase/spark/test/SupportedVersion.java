/*
 * Copyright (c) 2019 Couchbase, Inc.
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

/**
 * Represents the currently supported (tested) versions which can be used
 * in test setups.
 *
 * We are using the codenames to identify, but the ranges are written down
 * per enum to allow for proper range specs.
 *
 * @since 2.0.0
 */
public enum SupportedVersion {
  SPOCK(5, 1, 2),
  VULCAN(5, 5, 2);
  // ALICE(6, 0, 0),
  // MAD_HATTER(6, 5, 0);


  /**
   * Returns the currently latest supported version.
   */
  public static SupportedVersion latest() {
    return VULCAN;
  }

  private final int major;
  private final int minor;
  private final int patch;

  SupportedVersion(int major, int minor, int patch) {
    this.major = major;
    this.minor = minor;
    this.patch = patch;
  }

  public String containerVersion() {
    return major + "." + minor + "." + patch;
  }

  public int major() {
    return major;
  }

  public int minor() {
    return minor;
  }

  public int patch() {
    return patch;
  }

  public static SupportedVersion fromString(final String version) {
    if (version.equalsIgnoreCase("VULCAN")) {
      return VULCAN;
    } else if (version.equalsIgnoreCase("SPOCK")) {
      return SPOCK;
    } else {
      throw new UnsupportedOperationException("The given version is not supported/known. "
        + "Please check the SupportedVersion enum for supported versions.");
    }
  }
}
