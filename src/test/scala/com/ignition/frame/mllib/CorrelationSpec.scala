package com.ignition.frame.mllib

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

import com.ignition.frame.{ DataGrid, FrameFlowSpecification }
import com.ignition.types.{ RichStructType, double, fieldToRichStruct, int, string }

@RunWith(classOf[JUnitRunner])
class CorrelationSpec extends FrameFlowSpecification {

  val schema = string("color") ~ int("height") ~ int("weight") ~ int("iq")
  val grid = DataGrid(schema) rows (
    ("red", 175, 80, 105), ("red", 160, 55, 111), ("green", 182, 90, 70),
    ("blue", 172, 68, 84), ("blue", 194, 105, 68), ("green", 177, 75, 118),
    ("blue", 165, 70, 97), ("red", 159, 65, 75), ("green", 188, 82, 72),
    ("green", 174, 82, 120))

  "Correlation" should {
    "compute stats without grouping" in {
      val corr = Correlation("height", "weight", "iq")
      grid --> corr

      assertSchema(
        double("corr_height_weight") ~ double("corr_height_iq") ~ double("corr_weight_iq"),
        corr, 0)
    }
    "compute stats with grouping" in {
      val corr = Correlation() add ("height", "weight", "iq") groupBy "color"
      grid --> corr

      assertSchema(string("color") ~
        double("corr_height_weight") ~ double("corr_height_iq") ~ double("corr_weight_iq"),
        corr, 0)
    }
    "save to/load from xml" in {
      val s1 = Correlation("height", "weight", "iq")
      s1.toXml must ==/(
        <correlation method="pearson">
          <aggregate>
            <field name="height"/><field name="weight"/><field name="iq"/>
          </aggregate>
        </correlation>)
      Correlation.fromXml(s1.toXml) === s1

      val s2 = Correlation() add ("height", "weight", "iq") groupBy "color"
      s2.toXml must ==/(
        <correlation method="pearson">
          <aggregate>
            <field name="height"/><field name="weight"/><field name="iq"/>
          </aggregate>
          <group-by>
            <field name="color"/>
          </group-by>
        </correlation>)
      Correlation.fromXml(s2.toXml) === s2
    }
    "save to/load from json" in {
      import org.json4s.JsonDSL._

      val s1 = Correlation("height", "weight", "iq")
      s1.toJson === ("tag" -> "correlation") ~ ("aggregate" -> List("height", "weight", "iq")) ~
        ("method" -> "pearson") ~ ("groupBy" -> jNone)
      Correlation.fromJson(s1.toJson) === s1

      val s2 = Correlation() add ("height", "weight", "iq") groupBy "color"
      s2.toJson === ("tag" -> "correlation") ~ ("aggregate" -> List("height", "weight", "iq")) ~
        ("method" -> "pearson") ~ ("groupBy" -> List("color"))
      Correlation.fromJson(s2.toJson) === s2
    }
    "be unserializable" in assertUnserializable(Correlation())
  }
}