package com.ignition.workflow

import org.junit.runner.RunWith
import org.slf4j.LoggerFactory
import org.specs2.ScalaCheck
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class StepSpec extends Specification with ScalaCheck {

  val log = LoggerFactory.getLogger(getClass)

  implicit val ec: Unit = {}

  "step0 workflow" should {
    "yield the result" in prop { (a: Int) =>
      val step = new Step0[Int, Unit] { def compute(ec: Unit) = a }
      step.output === a
    }
  }

  "step0 step1 workflow" should {
    "yield the result" in prop { (a: Int) =>
      val stepA = new Step0[Int, Unit] { def compute(ec: Unit) = a }
      val stepB = new Step1[Int, Int, Unit] { def compute(ec: Unit)(arg: Int) = arg * 2 }
      stepB.connectFrom(stepA)
      stepB.output === a * 2
    }
  }

  "step0 step1 step2 workflow" should {
    "yield the result" in prop { (a: Int, b: Int) =>
      val stepA = new Step0[Int, Unit] { def compute(ec: Unit) = a }
      val stepB = new Step0[String, Unit] { def compute(ec: Unit) = b.toString }
      val stepC = new Step1[Int, Int, Unit] { def compute(ec: Unit)(arg: Int) = arg * 3 }
      val stepD = new Step1[String, Int, Unit] { def compute(ec: Unit)(arg: String) = arg.toInt }
      val stepE = new Step2[Int, Int, Int, Unit] { def compute(ec: Unit)(arg1: Int, arg2: Int) = arg1 - arg2 }
      stepC.connectFrom(stepA)
      stepD.connectFrom(stepB)
      stepE.connect1From(stepC)
      stepE.connect2From(stepD)
      stepE.output === a * 3 - b
    }
  }

  "step0 stepN workflow" should {
    "yield the result" in prop { (a: Int, b: Int, c: Int, d: Int, e: Int) =>
      val stepA = new Step0[Int, Unit] { def compute(ec: Unit) = a }
      val stepB = new Step0[Int, Unit] { def compute(ec: Unit) = b }
      val stepC = new Step0[Int, Unit] { def compute(ec: Unit) = c }
      val stepD = new Step0[Int, Unit] { def compute(ec: Unit) = d }
      val stepE = new Step0[Int, Unit] { def compute(ec: Unit) = e }
      val stepF = new StepN[Int, Int, Unit] { def compute(ec: Unit)(args: Iterable[Int]) = args.sum }
      stepF.connectFrom(stepA)
      stepF.connectFrom(stepB)
      stepF.connectFrom(stepC)
      stepF.connectFrom(stepD)
      stepF.connectFrom(stepE)
      stepF.output === a + b + c + d + e
    }
  }

  "disconnected steps" should {
    "throw an exception when evaluated" in {
      val step = new Step1[String, String, Unit]{ def compute(ec: Unit)(arg: String) = arg }
      step.output must throwA[WorkflowException]
    }
  }
}