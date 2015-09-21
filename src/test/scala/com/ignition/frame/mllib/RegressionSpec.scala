package com.ignition.frame.mllib

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

import com.ignition.frame.{ DataGrid, FrameFlowSpecification }
import com.ignition.types.{ RichStructType, double, fieldToRichStruct, int, string }

@RunWith(classOf[JUnitRunner])
class RegressionSpec extends FrameFlowSpecification {

  val schema = string("color") ~ int("x") ~ int("y") ~ int("z")
  val grid = DataGrid(schema) rows (
    ("red", 10, 21, 105), ("red", 20, 45, 111), ("green", 5, 12, 70),
    ("blue", 10, 18, 84), ("blue", 15, 27, 68), ("green", 15, 36, 118),
    ("blue", 30, 57, 97), ("red", 50, 98, 75), ("green", 40, 85, 72),
    ("green", 35, 76, 120))

  "Regression" should {
    "compute without grouping" in {
      val reg = Regression("x") features ("y", "z") iterations 200 step 5
      grid --> reg

      assertSchema(double("y_weight") ~ double("z_weight") ~ double("intercept") ~
        double("r2"), reg, 0)
    }
    "compute with grouping" in {
      val reg = Regression("x") features ("y", "z") groupBy "color" iterations 200 step 5
      grid --> reg

      assertSchema(string("color") ~ double("y_weight") ~ double("z_weight") ~
        double("intercept") ~ double("r2"), reg, 0)
    }
    "save to/load from xml" in {
      val reg = Regression("x") features ("y", "z") groupBy "color" iterations 200 step 5
      reg.toXml must ==/(
        <regression>
          <label>x</label>
          <features>
            <field name="y"/><field name="z"/>
          </features>
          <group-by>
            <field name="color"/>
          </group-by>
          <config>
            <method>LINEAR</method>
            <iterations>200</iterations>
            <step>5.0</step>
            <allowIntercept>false</allowIntercept>
          </config>
        </regression>)
      Regression.fromXml(reg.toXml) === reg
    }
    "save to/load from json" in {
      import org.json4s.JsonDSL._

      val reg = Regression("x") features ("y", "z") groupBy "color" iterations 200 step 5
      reg.toJson === ("tag" -> "regression") ~ ("label" -> "x") ~ ("features" -> List("y", "z")) ~
        ("groupBy" -> List("color")) ~ ("config" ->
          ("method" -> "LINEAR") ~ ("iterations" -> 200) ~ ("step" -> 5.0) ~ ("allowIntercept" -> false))
      Regression.fromJson(reg.toJson) === reg
    }
    "be unserializable" in assertUnserializable(Regression("x"))
  }
}