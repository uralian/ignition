package com.ignition

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SparkRuntimeSpec extends Specification with SparkTestHelper {

  "Variables" should {
    "be assignable by master and accessible by workers" in {
      val runtime = new SparkRuntime(ctx)

      runtime.vars("a") = 5
      runtime.vars("b") = "xyz"

      runtime.vars.names === Set("a", "b")
      runtime.vars("a") === 5
      runtime.vars.getAs[String]("b") === "xyz"

      val rdd = sc.parallelize(Seq(1, 2, 3, 4, 5, 6, 7, 8))
      val result1 = rdd map { x =>
        (x * runtime.vars.getAs[Int]("a"), runtime.vars.getAs[String]("b").toUpperCase)
      }

      result1.collect.toSet === Set((5, "XYZ"), (10, "XYZ"), (15, "XYZ"), (20, "XYZ"),
        (25, "XYZ"), (30, "XYZ"), (35, "XYZ"), (40, "XYZ"))
    }
    "be reassignable" in {
      val runtime = new SparkRuntime(ctx)

      runtime.vars("a") = 5
      runtime.vars("b") = "xyz"

      runtime.vars.names === Set("a", "b")
      runtime.vars("a") === 5
      runtime.vars.getAs[String]("b") === "xyz"

      runtime.vars("a") = 3
      runtime.vars.names === Set("a", "b")
      runtime.vars("a") === 3
      runtime.vars.getAs[String]("b") === "xyz"

      val rdd = sc.parallelize(Seq(1, 2, 3, 4, 5, 6, 7, 8))
      val result2 = rdd mapPartitions { nums =>
        nums map { n =>
          n * 2 + runtime.vars.getAs[Int]("a")
        }
      }

      result2.collect.toSet === Set(5, 7, 9, 11, 13, 15, 17, 19)
    }
    "be removable" in {
      val runtime = new SparkRuntime(ctx)

      runtime.vars("a") = 5
      runtime.vars.names === Set("a")

      runtime.vars.drop("a")
      runtime.vars.names must beEmpty
    }
    "fail on invalid name" in {
      val runtime = new SparkRuntime(ctx)
      runtime.vars("a") must throwA[Exception]

      val rdd = sc.parallelize(Seq(1, 2, 3, 4))
      rdd.map { _ => runtime.vars("x") }.collect must throwA[Exception]
    }
  }

  "Accumulators" should {
    "be assignable and read by master" in {
      val runtime = new SparkRuntime(ctx)

      runtime.accs("a") = 3
      runtime.accs("a") === 3
      runtime.accs.getAs[Int]("a") === 3
    }
    "be reassignable by master" in {
      val runtime = new SparkRuntime(ctx)

      runtime.accs("a") = 5
      runtime.accs.getAs[Int]("a") === 5

      runtime.accs("a") = 10
      runtime.accs.getAs[Int]("a") === 10
    }
    "be incrementable by workers" in {
      val runtime = new SparkRuntime(ctx)

      runtime.accs("a") = 0
      val rdd = sc.parallelize(Seq(1, 2, 3, 4, 5, 6, 7, 8))
      val result1 = rdd foreach { x =>
        runtime.accs.add("a", x)
      }

      runtime.accs("a") === 36
    }
  }
}