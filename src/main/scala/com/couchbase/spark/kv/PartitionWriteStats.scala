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

import org.apache.spark.internal.Logging

import java.util.concurrent.atomic.{AtomicInteger, LongAdder}
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
import scala.concurrent.duration._
import scala.util.control.NonFatal

/** `count` is every sample recorded, `dropped` how many of those were not kept for the
  * percentiles.
  */
private[kv] case class SampleSummary(
    count: Long,
    dropped: Long,
    min: Long,
    p50: Long,
    p95: Long,
    max: Long
)

/** Collects samples (e.g. latencies) from many threads, for summarising and resetting each stats
  * interval.
  *
  * Memory is bounded aggressively since this logic is being added in the context of OOM issues,
  * so we don't hold onto every sample.  Once `capacity` samples have been kept in an interval, any
  * more are dropped (and counted), so the percentiles then only cover the start of the interval.
  * `min`, `max` and `count` always cover every sample.
  *
  * The class is thread-safe as a thread-pool is writing to it.
  */
private[kv] final class Samples(capacity: Int) {
  private val kept    = new Array[Long](capacity)
  private var count   = 0L
  private var dropped = 0L
  private var min     = Long.MaxValue
  private var max     = Long.MinValue

  def record(value: Long): Unit = synchronized {
    if (value < min) min = value
    if (value > max) max = value
    if (count < capacity) kept(count.toInt) = value
    else dropped += 1
    count += 1
  }

  def summariseAndReset(): Option[SampleSummary] = {
    val (sorted, summary) = synchronized {
      if (count == 0) return None
      val copy   = java.util.Arrays.copyOf(kept, (count - dropped).toInt)
      val result = (copy, SampleSummary(count, dropped, min, 0, 0, max))
      count = 0
      dropped = 0
      min = Long.MaxValue
      max = Long.MinValue
      result
    }
    // Processed outside the lock, so writers are not held up.
    java.util.Arrays.sort(sorted)
    Some(
      summary.copy(p50 = Samples.percentile(sorted, 0.50), p95 = Samples.percentile(sorted, 0.95))
    )
  }
}

private[kv] object Samples {

  /** Nearest-rank percentile of a non-empty sorted array. */
  def percentile(sorted: Array[Long], p: Double): Long =
    sorted(math.max(0, math.ceil(p * sorted.length).toInt - 1))
}

/** Per-partition write statistics, logged every `interval` while the partition is being written.
  *
  * Each report covers the interval since the previous one, plus running totals for the partition.
  * It is safe to record from any thread.
  *
  * Individual write failures are also logged as they happen, with just the exception's message,
  * but only the first `maxFailuresLoggedPerInterval` in each interval (to avoid spamming). The rest are counted and the
  * number suppressed is included in the next report.
  */
private[kv] class PartitionWriteStats(
    val partition: String,
    reporterThreadName: String,
    interval: FiniteDuration = PartitionWriteStats.DefaultInterval,
    maxFailuresLoggedPerInterval: Int = PartitionWriteStats.DefaultMaxFailuresLoggedPerInterval
) extends Logging {
  import PartitionWriteStats._

  private val started              = System.nanoTime()
  private var intervalStarted      = started
  private val failuresThisInterval = new AtomicInteger()

  // Reset on each report.
  private val succeeded = new LongAdder
  private val failed    = new LongAdder
  private val ignored   = new LongAdder

  private val totalRead      = new LongAdder
  private val totalSucceeded = new LongAdder
  private val totalFailed    = new LongAdder
  private val totalIgnored   = new LongAdder

  // Successful KV operations only (to avoid timeouts skewing the data), in nanoseconds.
  private val opLatency = new Samples(SampleCapacity)
  // Per item on a worker thread, everything before the KV operation including JSON decode.  In nanoseconds.
  private val conversion = new Samples(SampleCapacity)
  // Time spent in Spark's `hasNext()` + `next()` per item, in nanoseconds. This is the upstream
  // work: reading files, fetching shuffle blocks, the `toJSON` encoding, etc.
  private val upstreamRead = new Samples(SampleCapacity)
  // Items waiting in the queue each time a new one is read. Consistently near the queue's capacity
  // means the workers (i.e. Couchbase) are the bottleneck; consistently near zero means upstream
  // Spark is.
  private val queueSize = new Samples(SampleCapacity)

  private var reporter: Option[ScheduledExecutorService] = None

  def recordUpstreamRead(nanos: Long, queueSizeBefore: Int): Unit = {
    totalRead.increment()
    upstreamRead.record(nanos)
    queueSize.record(queueSizeBefore)
  }

  def recordConversion(nanos: Long): Unit = conversion.record(nanos)

  def recordSuccess(latencyNanos: Long): Unit = {
    succeeded.increment()
    totalSucceeded.increment()
    opLatency.record(latencyNanos)
  }

  /** An operation that failed in an expected way and is deliberately ignored, such as a document
    * already existing under SaveMode.Ignore.
    */
  def recordIgnored(): Unit = {
    ignored.increment()
    totalIgnored.increment()
  }

  def recordFailure(documentId: String, err: Throwable): Unit = {
    failed.increment()
    totalFailed.increment()
    val n = failuresThisInterval.incrementAndGet()
    if (n <= maxFailuresLoggedPerInterval) {
      logWarning(s"Partition $partition: write of document '$documentId' failed: ${describe(err)}")
    } else if (n == maxFailuresLoggedPerInterval + 1) {
      logWarning(
        s"Partition $partition: more than $maxFailuresLoggedPerInterval write failures in this " +
          s"${interval.toSeconds}s interval, so no more will be logged individually until the " +
          "next stats report"
      )
    }
  }

  def start(): Unit = synchronized {
    val executor = Executors.newSingleThreadScheduledExecutor((r: Runnable) => {
      val thread = new Thread(r, reporterThreadName)
      thread.setDaemon(true)
      thread
    })
    // An exception escaping a periodic task silently cancels all its future runs.
    val task: Runnable = () =>
      try report(isFinal = false)
      catch { case NonFatal(e) => logWarning(s"Partition $partition: failed to report stats: $e") }
    executor.scheduleAtFixedRate(task, interval.toMillis, interval.toMillis, TimeUnit.MILLISECONDS)
    reporter = Some(executor)
  }

  /** Stops the periodic reports and logs a final one, covering the last partial interval. */
  def close(): Unit = {
    synchronized {
      reporter.foreach(_.shutdown())
      reporter = None
    }
    report(isFinal = true)
  }

  /** Logs, and returns, a report of the interval since the last one, and resets it. */
  private[kv] def report(isFinal: Boolean): String = synchronized {
    val now             = System.nanoTime()
    val intervalSeconds = (now - intervalStarted) / 1e9
    intervalStarted = now

    val s          = succeeded.sumThenReset()
    val f          = failed.sumThenReset()
    val i          = ignored.sumThenReset()
    val suppressed = math.max(0, failuresThisInterval.getAndSet(0) - maxFailuresLoggedPerInterval)

    val op       = opLatency.summariseAndReset()
    val convert  = conversion.summariseAndReset()
    val upstream = upstreamRead.summariseAndReset()
    val queue    = queueSize.summariseAndReset()
    // Every metric has the same capacity, so this is said once for the whole line.
    val subset = Seq(op, convert, upstream, queue).flatten.exists(_.dropped > 0)

    val line =
      s"Partition $partition: ${if (isFinal) "final" else "last"} ${f"$intervalSeconds%.1f"}s: " +
        s"succeeded=$s (${perSecond(s, intervalSeconds)}/s) failed=$f ignored=$i" +
        (if (suppressed > 0) s" (failure logs suppressed=$suppressed)" else "") +
        s" | op latency ${millis(op)}" +
        s" | conversion ${millis(convert)}" +
        s" | upstream read ${millis(upstream)}" +
        s" | queue size ${plain(queue)}" +
        (if (subset) s" (percentiles from first $SampleCapacity samples of each)" else "") +
        s" | totals in ${f"${(now - started) / 1e9}%.1f"}s: read=${totalRead.sum()} " +
        s"succeeded=${totalSucceeded.sum()} failed=${totalFailed.sum()} " +
        s"ignored=${totalIgnored.sum()}"

    logInfo(line)
    line
  }
}

private[kv] object PartitionWriteStats {
  val DefaultInterval: FiniteDuration          = 10.seconds
  val DefaultMaxFailuresLoggedPerInterval: Int = 3

  /** Per metric. Bounds the memory used, as a fast partition can complete hundreds of thousands of
    * operations in an interval.
    */
  val SampleCapacity: Int = 10000

  /** Just the exception's class and message, without the stacktrace. */
  def describe(err: Throwable): String = s"${err.getClass.getSimpleName}: ${err.getMessage}"

  private def perSecond(count: Long, seconds: Double): String =
    if (seconds <= 0) "0" else f"${count / seconds}%.0f"

  private def millis(summary: Option[SampleSummary]): String = summary match {
    case None => "n/a"
    case Some(s) =>
      def ms(nanos: Long) = f"${nanos / 1e6}%.2f"
      s"min=${ms(s.min)} p50=${ms(s.p50)} p95=${ms(s.p95)} max=${ms(s.max)}ms"
  }

  private def plain(summary: Option[SampleSummary]): String = summary match {
    case None    => "n/a"
    case Some(s) => s"min=${s.min} p50=${s.p50} p95=${s.p95} max=${s.max}"
  }
}
