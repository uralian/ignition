package com.ignition.workflow

import scala.collection.mutable.Queue
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext.IntAccumulatorParam
import org.apache.spark.streaming.{ Milliseconds, StreamingContext }
import org.junit.runner.RunWith
import org.slf4j.LoggerFactory
import org.specs2.mutable.Specification
import com.ignition.BeforeAllAfterAll
import com.ignition.workflow.dstream.core._
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DStreamStepSpec extends Specification with BeforeAllAfterAll {
  sequential

  val log = LoggerFactory.getLogger(getClass)

  implicit val sc = new SparkContext("local[4]", "test")

  override def afterAll() = {
    sc.stop
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.master.port")
  }

  "DStream Queue + Map + Filter" should {
    "yield the result" in {
      implicit val ssc = new StreamingContext(sc, Milliseconds(100))

      val rdd1 = sc.parallelize(Seq(1, 2, 3, 4))
      val rdd2 = sc.parallelize(Seq(5, 6, 7, 8))
      val queue = Queue(rdd1, rdd2)

      val stepA = new DStreamQueue(queue, true)
      val stepB = new DStreamMap[Int, Int](_ * 2)
      val stepC = new DStreamFilter[Int](_ % 3 == 1)
      stepA.connectTo(stepB).connectTo(stepC)

      val accum = sc.accumulator(0, "accum1")

      stepC.output.foreachRDD { rdd => rdd foreach { accum += _ } }

      ssc.start
      ssc.awaitTermination(300)
      ssc.stop(false)

      accum.value === 30
    }
  }

  "DStream Queue + FlatMap + ReduceByKey + Join" should {
    "yield the result" in {
      implicit val ssc = new StreamingContext(sc, Milliseconds(100))

      val queue1 = Queue(sc.parallelize(Seq(1, 2, 3)))
      val queue2 = Queue(sc.parallelize(Seq(1, 3)))

      val stepA = new DStreamQueue(queue1, true)
      val stepB = new DStreamQueue(queue2, true)
      val stepC = new DStreamFlatMap[Int, (Int, Int)](n => List(n -> n * 2, n + 1 -> n * 3))
      val stepD = new DStreamFlatMap[Int, (Int, String)](n => List((n -> n.toString)))
      val stepE = new DStreamReduceByKey[Int, Int]((a, b) => a + b)
      val stepF = new DStreamJoin[Int, Int, String]

      stepA.connectTo(stepC).connectTo(stepE).connectTo1(stepF)
      stepB.connectTo(stepD).connectTo2(stepF)

      val accum = sc.accumulator(0, "accum2")

      stepF.output.foreachRDD { rdd =>
        rdd foreach {
          case (a, (b, c)) => accum += (a + b + c.toInt)
        }
      }

      ssc.start
      ssc.awaitTermination(200)
      ssc.stop(false)

      accum.value === 22
    }
  }
}