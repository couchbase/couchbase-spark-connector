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
package com.couchbase.spark.query

import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertNotNull, assertNull, assertTrue}
import org.junit.jupiter.api.Test
import org.reactivestreams.Subscription
import reactor.core.publisher.{BaseSubscriber, Flux, Mono}
import reactor.core.scala.publisher.{SFlux, SMono}

import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import scala.collection.mutable.ArrayBuffer

class QueryPartitionReaderBackpressureTest {

  private class Recorder(initialRequest: Long) extends BaseSubscriber[String] {
    val received                       = ArrayBuffer.empty[String]
    val error                          = new AtomicReference[Throwable]()
    @volatile var completed            = false
    override def hookOnSubscribe(s: Subscription): Unit = request(initialRequest)
    override def hookOnNext(v: String): Unit             = received += v
    override def hookOnError(t: Throwable): Unit         = error.set(t)
    override def hookOnComplete(): Unit                  = completed = true
  }

  private def trackedRows(count: Int, requested: AtomicLong): SFlux[String] =
    SFlux.fromPublisher(
      Flux.range(1, count).map[String](i => i.toString).doOnRequest(n => requested.addAndGet(n))
    )

  @Test
  def onlyRequestedRowsArePulledFromUpstream(): Unit = {
    val requested    = new AtomicLong()
    val metaDataSeen = new AtomicReference[String]()

    val recorder = new Recorder(30)
    QueryPartitionReader
      .rowsThenMetaData(trackedRows(100000, requested), SMono.just("md"), (md: String) => metaDataSeen.set(md))
      .subscribe(recorder)

    assertEquals(30, requested.get(), "upstream rows should only see the downstream demand")
    assertEquals(30, recorder.received.size)
    assertNull(metaDataSeen.get(), "metadata should not be consumed before the rows are")
    assertFalse(recorder.completed)

    recorder.request(20)
    assertEquals(50, requested.get())
    assertEquals(50, recorder.received.size)
    recorder.dispose()
  }

  @Test
  def metaDataIsConsumedAfterAllRowsAndBeforeCompletion(): Unit = {
    val requested            = new AtomicLong()
    val rowsSeenAtMetaData   = new AtomicLong(-1)
    val recorder             = new Recorder(Long.MaxValue)

    QueryPartitionReader
      .rowsThenMetaData(
        trackedRows(1000, requested),
        SMono.just("md"),
        (_: String) => rowsSeenAtMetaData.set(recorder.received.size)
      )
      .subscribe(recorder)

    assertEquals(1000, recorder.received.size)
    assertEquals((1 to 1000).map(_.toString), recorder.received.toSeq)
    assertEquals(1000, rowsSeenAtMetaData.get())
    assertTrue(recorder.completed)
    assertNull(recorder.error.get())
  }

  @Test
  def rowErrorIsPropagatedAndMetaDataSkipped(): Unit = {
    val metaDataSubscribed = new AtomicReference[java.lang.Boolean](false)
    val recorder           = new Recorder(Long.MaxValue)
    val rows = SFlux.fromPublisher(
      Flux.just("1", "2").concatWith(Mono.error[String](new RuntimeException("query failed")))
    )
    val metaData = SMono.fromPublisher(Mono.fromCallable(() => {
      metaDataSubscribed.set(true)
      "md"
    }))

    QueryPartitionReader.rowsThenMetaData(rows, metaData, (_: String) => ()).subscribe(recorder)

    assertEquals(Seq("1", "2"), recorder.received.toSeq)
    assertNotNull(recorder.error.get())
    assertEquals("query failed", recorder.error.get().getMessage)
    assertFalse(metaDataSubscribed.get())
    assertFalse(recorder.completed)
  }
}
