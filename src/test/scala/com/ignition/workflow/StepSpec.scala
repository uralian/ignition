package com.ignition.workflow

import org.junit.runner.RunWith
import org.specs2.ScalaCheck
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class StepSpec extends Specification with ScalaCheck {

  implicit val ec: Unit = {}

  def throwRT() = throw new RuntimeException("runtime")
  def throwWF() = throw WorkflowException("workflow")

  "step0 workflow" should {
    "yield the result" in prop { (a: Int) =>
      val step = new Step0[Int, Unit] { def compute(ec: Unit) = a }
      step.output === a
    }
    "wrap runtime error into workflow exception" in {
      val step = new Step0[Int, Unit] { def compute(ec: Unit) = throwRT }
      step.output must throwA[WorkflowException](message = "Step computation failed")
    }
    "propagate workflow exception" in {
      val step = new Step0[Int, Unit] { def compute(ec: Unit) = throwWF }
      step.output must throwA[WorkflowException](message = "workflow")
    }
  }

  "step1" should {
    "wrap runtime error into workflow exception" in {
      val stepA = new Step0[Int, Unit] { def compute(ec: Unit) = 1 }
      val stepB = new Step1[Int, Int, Unit] { def compute(ec: Unit)(arg: Int) = throwRT }
      stepB.connectFrom(stepA)
      stepB.output must throwA[WorkflowException](message = "Step computation failed")
    }
    "propagate workflow exception" in {
      val stepA = new Step0[Int, Unit] { def compute(ec: Unit) = 1 }
      val stepB = new Step1[Int, Int, Unit] { def compute(ec: Unit)(arg: Int) = throwWF }
      stepB.connectFrom(stepA)
      stepB.output must throwA[WorkflowException](message = "workflow")
    }
    "throw workflow exception when not connected" in {
      val stepB = new Step1[Int, Int, Unit] { def compute(ec: Unit)(arg: Int) = arg }
      stepB.output must throwA[WorkflowException](message = "Input is not connected")
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

  "step2" should {
    "wrap runtime error into workflow exception" in {
      val stepA = new Step0[Int, Unit] { def compute(ec: Unit) = 1 }
      val stepB = new Step0[Int, Unit] { def compute(ec: Unit) = 1 }
      val stepC = new Step2[Int, Int, Int, Unit] { def compute(ec: Unit)(arg1: Int, arg2: Int) = throwRT }
      stepC.connect1From(stepA)
      stepC.connect2From(stepB)
      stepC.output must throwA[WorkflowException](message = "Step computation failed")
    }
    "propagate workflow exception" in {
      val stepA = new Step0[Int, Unit] { def compute(ec: Unit) = 1 }
      val stepB = new Step0[Int, Unit] { def compute(ec: Unit) = 1 }
      val stepC = new Step2[Int, Int, Int, Unit] { def compute(ec: Unit)(arg1: Int, arg2: Int) = throwWF }
      stepC.connect1From(stepA)
      stepC.connect2From(stepB)
      stepC.output must throwA[WorkflowException](message = "workflow")
    }
    "throw workflow exception when not connected" in {
      val stepA = new Step0[Int, Unit] { def compute(ec: Unit) = 1 }
      val stepC = new Step2[Int, Int, Int, Unit] { def compute(ec: Unit)(arg1: Int, arg2: Int) = arg1 + arg2 }
      stepC.output must throwA[WorkflowException](message = "Input1 is not connected")
      stepC.connect1From(stepA)
      stepC.output must throwA[WorkflowException](message = "Input2 is not connected")
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

  "stepN" should {
    "wrap runtime error into workflow exception" in {
      val stepA = new Step0[Int, Unit] { def compute(ec: Unit) = 1 }
      val stepB = new Step0[Int, Unit] { def compute(ec: Unit) = 1 }
      val stepF = new StepN[Int, Int, Unit] { def compute(ec: Unit)(args: Iterable[Int]) = throwRT }
      stepF.connectFrom(stepA)
      stepF.connectFrom(stepB)
      stepF.output must throwA[WorkflowException](message = "Step computation failed")
    }
    "propagate workflow exception" in {
      val stepA = new Step0[Int, Unit] { def compute(ec: Unit) = 1 }
      val stepB = new Step0[Int, Unit] { def compute(ec: Unit) = 1 }
      val stepF = new StepN[Int, Int, Unit] { def compute(ec: Unit)(args: Iterable[Int]) = throwWF }
      stepF.connectFrom(stepA)
      stepF.connectFrom(stepB)
      stepF.output must throwA[WorkflowException](message = "workflow")
    }
    "throw workflow exception when not connected" in {
      val stepF = new StepN[Int, Int, Unit] { def compute(ec: Unit)(args: Iterable[Int]) = args.sum }
      stepF.output must throwA[WorkflowException](message = "No inputs connected")
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
}