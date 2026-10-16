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

import org.junit.jupiter.api.Assertions.{assertEquals, assertSame, assertThrows, assertTrue}
import org.junit.jupiter.api.{Test, Timeout}

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

// Guards against the worker threads never exiting, which would otherwise hang the build.
@Timeout(60)
class WriteAllTest {

  private def threadName(i: Int) = s"write-all-test-$i"

  // Not started, so it records but never reports on its own.
  private def stats = new PartitionWriteStats("test", "write-all-test-stats")

  /** Simulates a blocking write, tracking how many are running at once. */
  private def blockingOp(inFlight: AtomicInteger, maxSeen: AtomicInteger): Unit = {
    val now = inFlight.incrementAndGet()
    maxSeen.accumulateAndGet(now, Math.max)
    Thread.sleep(2)
    inFlight.decrementAndGet()
  }

  @Test
  def itemsOnlyConsumedOnCallingThreadAndProcessedOnWorkers(): Unit = {
    val caller    = Thread.currentThread()
    val inFlight  = new AtomicInteger()
    val maxSeen   = new AtomicInteger()
    val processed = ConcurrentHashMap.newKeySet[String]()

    val items = (1 to 500).iterator.map(i => {
      assertSame(caller, Thread.currentThread())
      i.toString
    })

    val count = RelationPartitionWriter.writeAll(items, 8, threadName, stats) { item =>
      assertTrue(Thread.currentThread().getName.startsWith("write-all-test-"))
      blockingOp(inFlight, maxSeen)
      processed.add(item)
    }

    assertEquals(500L, count)
    assertEquals(500, processed.size())
    assertEquals(0, inFlight.get())
    assertTrue(maxSeen.get() <= 8, s"max in-flight was ${maxSeen.get()}")
    assertTrue(maxSeen.get() > 1, s"max in-flight was ${maxSeen.get()}")
  }

  @Test
  def emptyInput(): Unit = {
    assertEquals(0L, RelationPartitionWriter.writeAll(Iterator.empty[String], 4, threadName, stats)(_ => ()))
  }

  @Test
  def firstProcessErrorStopsIterationAndIsRethrown(): Unit = {
    val consumed = new AtomicInteger()
    val inFlight = new AtomicInteger()
    val maxSeen  = new AtomicInteger()
    val failure  = new IllegalStateException("boom")

    val items = (1 to 10000).iterator.map(i => { consumed.incrementAndGet(); i.toString })

    val thrown = assertThrows(
      classOf[IllegalStateException],
      () =>
        RelationPartitionWriter.writeAll(items, 4, threadName, stats) { item =>
          if (item == "10") throw failure
          blockingOp(inFlight, maxSeen)
        }
    )

    assertSame(failure, thrown)
    assertTrue(consumed.get() < 10000, s"consumed ${consumed.get()} items")
    assertEquals(0, inFlight.get())
  }

  @Test
  def exceptionProducingItemWaitsForInFlightThenPropagates(): Unit = {
    val inFlight = new AtomicInteger()
    val maxSeen  = new AtomicInteger()

    val items = (1 to 100).iterator.map(i => {
      if (i == 20) throw new IllegalArgumentException("upstream failure")
      i.toString
    })

    assertThrows(
      classOf[IllegalArgumentException],
      () => RelationPartitionWriter.writeAll(items, 4, threadName, stats)(_ => blockingOp(inFlight, maxSeen))
    )
    assertEquals(0, inFlight.get())
  }
}
