/*
 * Copyright (c) 2026 Couchbase, Inc.
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
package com.couchbase.spark.kv

import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.Test

class PartitionWriteStatsTest {

  @Test
  def exactPercentilesUnderCapacity(): Unit = {
    val samples = new Samples(1000)
    scala.util.Random.shuffle((1L to 100L).toList).foreach(samples.record)

    assertEquals(Some(SampleSummary(100, 0, 1, 50, 95, 100)), samples.summariseAndReset())
  }

  @Test
  def resetsAfterSummarising(): Unit = {
    val samples = new Samples(10)
    samples.record(5)
    samples.summariseAndReset()

    assertEquals(None, samples.summariseAndReset())

    samples.record(7)
    assertEquals(Some(SampleSummary(1, 0, 7, 7, 7, 7)), samples.summariseAndReset())
  }

  @Test
  def overCapacityKeepsFirstSamplesForPercentilesButMinMaxCountCoverAll(): Unit = {
    val samples = new Samples(100)
    (1L to 1000L).foreach(samples.record)

    // Percentiles only from 1 to 100, the rest are dropped.
    assertEquals(Some(SampleSummary(1000, 900, 1, 50, 95, 1000)), samples.summariseAndReset())

    // And the dropped count resets with the interval.
    samples.record(5)
    assertEquals(Some(SampleSummary(1, 0, 5, 5, 5, 5)), samples.summariseAndReset())
  }

  @Test
  def reportShowsWhenPercentilesAreFromASubset(): Unit = {
    val stats = new PartitionWriteStats("1 of 4", "unused")
    (1 to PartitionWriteStats.SampleCapacity + 5).foreach(_ => stats.recordSuccess(1000000))

    val report = stats.report(isFinal = false)
    val note   = s"(percentiles from first ${PartitionWriteStats.SampleCapacity} samples of each)"
    assertTrue(report.contains(note), report)
    // Once for the line, not once per metric.
    assertEquals(1, report.sliding(note.length).count(_ == note), report)

    // And not at all once an interval is back under capacity.
    stats.recordSuccess(1000000)
    assertFalse(stats.report(isFinal = false).contains("percentiles from"))
  }

  @Test
  def reportCoversIntervalThenResetsButKeepsTotals(): Unit = {
    val stats = new PartitionWriteStats("1 of 4", "unused")
    stats.recordUpstreamRead(1000000, 3)
    stats.recordSuccess(2000000)
    stats.recordSuccess(4000000)
    stats.recordIgnored()
    stats.recordFailure("doc-1", new RuntimeException("boom"))

    val first = stats.report(isFinal = false)
    assertTrue(first.contains("succeeded=2 "), first)
    assertTrue(first.contains("failed=1 ignored=1"), first)
    assertTrue(first.contains("op latency min=2.00 p50=2.00 p95=4.00 max=4.00ms"), first)
    assertTrue(first.contains("queue size min=3 p50=3 p95=3 max=3"), first)
    assertTrue(first.contains("read=1 succeeded=2 failed=1 ignored=1"), first)

    val second = stats.report(isFinal = true)
    assertTrue(second.contains("final"), second)
    assertTrue(second.contains("succeeded=0 "), second)
    assertTrue(second.contains("op latency n/a"), second)
    assertTrue(second.contains("read=1 succeeded=2 failed=1 ignored=1"), second)
  }

  @Test
  def failureLogsSuppressedAfterLimitPerInterval(): Unit = {
    val stats = new PartitionWriteStats("1 of 4", "unused", maxFailuresLoggedPerInterval = 3)
    (1 to 5).foreach(i => stats.recordFailure(s"doc-$i", new RuntimeException("boom")))

    val first = stats.report(isFinal = false)
    assertTrue(first.contains("failed=5"), first)
    assertTrue(first.contains("failure logs suppressed=2"), first)

    // The limit applies per interval.
    stats.recordFailure("doc-6", new RuntimeException("boom"))
    val second = stats.report(isFinal = false)
    assertTrue(second.contains("failed=1"), second)
    assertFalse(second.contains("suppressed"), second)
  }
}
