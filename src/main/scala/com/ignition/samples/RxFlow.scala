package com.ignition.samples

import _root_.rx.lang.scala._
import com.ignition.util.Logging
import scala.concurrent.duration._
import com.ignition.rx.core._
import com.ignition.rx._
import rx.lang.scala.subjects.BehaviorSubject
import scala.util.Random

object RxFlow extends App with Logging {

  testZero
  testValueHolder
  testInterval
  testSequence
  testTimer
  testRange
  testAMB
  testCombineLatest
  testCombineLatest2
  testCombineLatest3
  testZip
  testCache
  testCollect
  testDelay
  testDistinct
  testDropByTime
  testDropByCount
  testDropWhile

  def testZero() = {
    val zero = new Zero[Int]
    zero.output subscribe testSub("ZERO")
    zero.reset
    delay(100)
    zero.reset
    delay(100)
  }

  def testValueHolder() = {
    val vh = new ValueHolder[Int]
    vh.output subscribe testSub("VALUE_HOLDER")
    vh.value <~ 100
    200 ~> vh.value
    vh.reset

    val vh2 = new ValueHolder[Int]
    vh2.value.set(300)
    vh.value <~ vh2
    vh2.reset
    400 ~> vh2.value
    vh2.reset

    vh2.value.set(500)
    vh2.reset
  }

  def testInterval() = {
    val interval = new Interval
    interval.output subscribe testSub("INTERVAL")

    interval.initial <~ (50 milliseconds)
    interval.period <~ (100 milliseconds)
    interval.reset
    delay(500)

    interval.period.set(200 milliseconds)
    interval.reset
    delay(400)

    val vh = new ValueHolder[Duration]
    (150 milliseconds) ~> vh.value
    vh ~> interval.period
    vh.reset
    delay(400)
  }

  def testSequence() = {
    val seq = new Sequence[String]
    seq.output subscribe testSub("SEQUENCE")

    seq.items <~ ("abc", "xyz")
    seq.reset
    delay(50)
    seq.items.add <~ "zzz"
    seq.items(1) <~ "123"
    seq.reset
    delay(50)
    seq.items.removeLast
    seq.reset
    delay(50)
  }

  def testTimer() = {
    val timer = new Timer
    timer.output subscribe testSub("TIMER")

    timer.delay <~ (100 milliseconds)
    timer.reset
    delay(200)

    val vh = new ValueHolder[Duration]
    vh.value <~ (200 milliseconds)
    vh ~> timer.delay
    vh.reset
    delay(300)
  }

  def testRange() = {
    val rng = new Range[Int]
    rng.output subscribe testSub("RANGE")

    rng.range <~ (5 to 20 by 4)
    rng.reset
  }

  def testAMB() = {
    val amb = new AMB[Long]
    amb.output subscribe testSub("AMB")

    val i1 = new Interval
    i1.initial <~ (200 milliseconds)
    i1.period <~ (50 milliseconds)

    val i2 = new Interval
    i2.initial <~ (100 milliseconds)
    i2.period <~ (200 milliseconds)

    val i3 = new Interval
    i3.initial <~ (300 milliseconds)
    i3.period <~ (400 milliseconds)

    amb.sources.add(3)
    i1 ~> amb.sources(0)
    i2 ~> amb.sources(1)
    i3 ~> amb.sources(2)

    i1.reset
    i2.reset
    i3.reset
    delay(500)
  }

  def testCombineLatest() = {
    val cmb = new CombineLatest[Long]
    cmb.output subscribe testSub("COMBINE")

    val i1 = new Interval
    i1.initial <~ (0 milliseconds)
    i1.period <~ (50 milliseconds)

    val vh = new ValueHolder[Long]
    vh.value <~ 55

    cmb.sources.add(2)
    cmb.sources(0) <~ i1
    cmb.sources(1) <~ vh

    i1.reset
    vh.reset
    delay(100)

    vh.value <~ 66
    vh.reset
    delay(100)

    i1.period <~ (20 milliseconds)
    i1.reset
    delay(100)
  }

  def testCombineLatest2() = {
    val cmb = new CombineLatest2[Int, String]
    cmb.output subscribe testSub("COMBINE")

    cmb.source1 <~ 100
    cmb.source2 <~ "hello"
    cmb.reset
    delay(100)

    cmb.source2 <~ "world"
    cmb.reset
    delay(100)
  }

  def testCombineLatest3() = {
    val cmb = new CombineLatest3[Long, String, Boolean]
    cmb.output subscribe testSub("COMBINE")

    val i1 = new Interval
    i1.initial <~ (50 milliseconds)
    i1.period <~ (100 milliseconds)

    cmb.source1 <~ i1
    cmb.source2 <~ "hello"
    cmb.source3 <~ true
    i1.reset
    delay(200)

    cmb.source2 <~ "world"
    cmb.reset
    delay(200)

    cmb.source3 <~ false
    cmb.reset
    delay(100)
  }

  def testZip() = {
    val zip = new Zip[Long]
    zip.output subscribe testSub("ZIP")

    val i1 = new Interval
    i1.initial <~ (50 milliseconds)
    i1.period <~ (100 milliseconds)

    val i2 = new Interval
    i2.initial <~ (100 milliseconds)
    i2.period <~ (50 milliseconds)

    zip.sources.add(2)
    zip.sources(0) <~ i1
    zip.sources(1) <~ i2
    i1.reset
    i2.reset
    delay(300)

    i2.period <~ (10 milliseconds)
    i2.reset
    delay(500)
    zip.shutdown

    val zi = new ZipWithIndex[String]
    zi.output subscribe testSub("ZIP_INDEX")

    val rng = new Range[String]
    rng ~> zi.source
    rng.range <~ Seq("abc", "xyz", "123")
    rng.reset
    delay(300)
  }

  def testCache() = {
    val i1 = new Interval
    i1.initial <~ (0 milliseconds)
    i1.period <~ (50 milliseconds)

    val cache = new Cache[Long]
    cache.output subscribe testSub("CACHE1")
    Some(10) ~> cache.capacity
    i1 ~> cache.source
    i1.reset
    delay(200)

    cache.output subscribe testSub("CACHE2")
    delay(200)
  }

  def testCollect() = {
    val i1 = new Interval
    i1.initial <~ (0 milliseconds)
    i1.period <~ (100 milliseconds)
    i1.reset
    delay(300)

    val collect = new Collect[Long, String]
    collect.output subscribe testSub("COLLECT")

    i1 ~> collect.source

    val even: PartialFunction[Long, String] = {
      case x if x % 2 == 0 => s"$x: even"
    }

    val mul: PartialFunction[Long, String] = {
      case x if x % 3 == 0 => s"$x: *3"
      case x if x % 4 == 0 => s"$x: *4"
      case x if x % 5 == 0 => s"$x: *5"
    }

    collect.selector <~ even
    collect.reset
    delay(500)

    collect.selector <~ mul
    collect.reset
    delay(800)

    i1.reset
    delay(500)
  }

  def testDelay() = {
    val rng = new Range[Int]

    val del = new Delay[Int]
    del.output subscribe testSub("DELAY")

    del.source <~ rng
    del.period <~ (500 milliseconds)

    rng.range <~ (1 to 15)
    rng.reset
    delay(700)
  }

  def testDistinct() = {
    val rng = new Range[Int]

    val dis = new Distinct[Int]
    dis.output subscribe testSub("DISTINCT")

    dis.source <~ rng
    dis.selector <~ ((n: Int) => n / 2)
    dis.global <~ true

    rng.range <~ (1 to 20)
    rng.reset

    dis.selector <~ ((n: Int) => n % 3)
    rng.reset

    dis.global <~ false
    dis.selector <~ ((n: Int) => n)
    rng.range <~ Seq(1, 1, 1, 2, 3, 4, 4)
    rng.reset
  }

  def testDropByTime() = {
    val i1 = new Interval
    i1.initial <~ (0 milliseconds)
    i1.period <~ (100 milliseconds)

    val drop = new DropByTime[Long]
    drop.output subscribe testSub("DROP")

    drop.source <~ i1
    drop.period <~ (200 milliseconds)

    i1.reset
    delay(500)

    i1.period <~ (50 milliseconds)
    i1.reset
    delay(400)
  }

  def testDropByCount() = {
    val rng = new Range[Int]

    val drop = new DropByCount[Int]
    drop.output subscribe testSub("DROP")

    drop.count <~ 4
    drop.source <~ rng

    rng.range <~ (1 to 10)
    rng.reset
  }

  def testDropWhile() = {
    val rng = new Range[Int]

    val drop = new DropWhile[Int]
    drop.output subscribe testSub("DROP")

    drop.predicate <~ ((n: Int) => n < 5)
    drop.source <~ rng

    rng.range <~ (1 to 10)
    rng.reset
  }

  /* helpers */

  def testSub[T](name: String) = Subscriber[T](
    (x: T) => info(s"$name: $x"),
    (err: Throwable) => error(s"$name: $err", err),
    () => info(s"$name: done"))

  def delay(millis: Long): Unit = delay(millis milliseconds)

  def delay(duration: Duration): Unit = Thread.sleep(duration.toMillis)
}