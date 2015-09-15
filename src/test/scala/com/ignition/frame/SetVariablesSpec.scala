package com.ignition.frame

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SetVariablesSpec extends FrameFlowSpecification {
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
    "save to/load from xml" in {
      val sv = SetVariables("a" -> 5, "b" -> true, "c" -> null)
      sv.toXml must ==/(
        <set-variables>
          <var name="a" type="integer">5</var>
          <var name="b" type="boolean">true</var>
          <var name="c"/>
        </set-variables>)
      SetVariables.fromXml(sv.toXml) === sv
    }
    "save to/load from json" in {
      import org.json4s.JsonDSL._

      val sv = SetVariables("a" -> 5, "b" -> true, "c" -> null)
      sv.toJson === ("tag" -> "set-variables") ~ ("vars" -> List(
        ("name" -> "a") ~ ("type" -> "integer") ~ ("value" -> 5),
        ("name" -> "b") ~ ("type" -> "boolean") ~ ("value" -> true),
        ("name" -> "c") ~ ("type" -> jNone) ~ ("value" -> null)))
      SetVariables.fromJson(sv.toJson) === sv
    }
    "be unserializable" in assertUnserializable(SetVariables())
  }
}