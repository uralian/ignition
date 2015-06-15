package com.ignition.stream

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SetVariablesSpec extends StreamFlowSpecification {
  sequential

  "SetVariables" should {
    "initialize broadcasts" in {
      val sv = SetVariables("a" -> 5, "b" -> true)
      sv.output

      rt.vars.names === Set("a", "b")
      rt.vars("a") === 5
      rt.vars("b") === true
    }
    "replace broadcasts" in {
      val sv1 = SetVariables("a" -> 5, "b" -> true)
      val sv2 = SetVariables("a" -> 99, "c" -> "hello")
      sv1 --> sv2
      sv2.output

      rt.vars.names === Set("a", "b", "c")
      rt.vars("a") === 99
      rt.vars("b") === true
      rt.vars("c") === "hello"
    }
    "drop broadcasts" in {
      val sv1 = SetVariables("a" -> 5, "b" -> true)
      val sv2 = SetVariables("a" -> 99, "b" -> null)
      sv1 --> sv2
      sv2.output

      rt.vars.names === Set("a", "c")
      rt.vars("a") === 99
      rt.vars("c") === "hello"
    }
  }
}