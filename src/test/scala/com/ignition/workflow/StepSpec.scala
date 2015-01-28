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
      val step = new Step0[Int, Unit](_ => a) {}
      step.output === a
    }
  }

  "step0 step1 workflow" should {
    "yield the result" in prop { (a: Int) =>
      val stepA = new Step0[Int, Unit](_ => a) {}
      val stepB = new Step1[Int, Int, Unit](_ => _ * 2) {}
      stepB.connectFrom(stepA)
      stepB.output === a * 2
    }
  }

  "step0 step1 step2 workflow" should {
    "yield the result" in prop { (a: Int, b: Int) =>
      val stepA = new Step0[Int, Unit](_ => a) {}
      val stepB = new Step0[String, Unit](_ => b.toString) {}
      val stepC = new Step1[Int, Int, Unit](_ => _ * 3) {}
      val stepD = new Step1[String, Int, Unit](_ => _.toInt) {}
      val stepE = new Step2[Int, Int, Int, Unit](_ => _ - _) {}
      stepC.connectFrom(stepA)
      stepD.connectFrom(stepB)
      stepE.connect1From(stepC)
      stepE.connect2From(stepD)
      stepE.output === a * 3 - b
    }
  }

  "step0 stepN workflow" should {
    "yield the result" in prop { (a: Int, b: Int, c: Int, d: Int, e: Int) =>
      val stepA = new Step0[Int, Unit](_ => a) {}
      val stepB = new Step0[Int, Unit](_ => b) {}
      val stepC = new Step0[Int, Unit](_ => c) {}
      val stepD = new Step0[Int, Unit](_ => d) {}
      val stepE = new Step0[Int, Unit](_ => e) {}
      val stepF = new StepN[Int, Int, Unit](_ => _.sum) {}
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
      val step = new Step1[String, String, Unit](_ => identity[String]) {}
      step.output must throwA[WorkflowException]
    }
  }
}