package com.ignition.workflow.grid

import scala.Array.canBuildFrom

import org.junit.runner.RunWith
import org.specs2.matcher.XmlMatchers
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import com.ignition.SparkTestHelper
import com.ignition.data.{ columnInfo2metaData, double, int, string }
import com.ignition.scripting.RichString
import com.ignition.workflow.rdd.grid.Formula
import com.ignition.workflow.rdd.grid.input.DataGridInput

@RunWith(classOf[JUnitRunner])
class FormulaSpec extends Specification with SparkTestHelper with XmlMatchers {
  sequential

  val meta = string("info") ~ string("data") ~ int("count") ~ double("weight")
  val row1 = meta.row("<a>X</a>", """{"a":5, "b":{"c":"none"}}""", 3, 62.15)
  val row2 = meta.row("<a>Y</a>", """{"a":1, "b":"none"}""", 8, 0.33)
  val grid = DataGridInput(meta).addRow(row1).addRow(row2)

  "Formula" should {
    "process JSON field epressions" in {
      val formula = Formula("x" -> "$.a".json("data"))
      formula.connectFrom(grid)
      formula.outMetaData === Some(meta ~ string("x"))
      formula.output.collect.map(_.getString("x")).toSet === Set("5", "1")
    }
    "process XPath field expressions" in {
      val formula = Formula("y" -> "a".xpath("info"))
      formula.connectFrom(grid)
      formula.outMetaData === Some(meta ~ string("y"))
      formula.output.collect.map(_.getString("y")).toSet === Set("<a>X</a>", "<a>Y</a>")
    }
    "process MVEL field expressions" in {
      val formula = Formula("z" -> "count * weight".mvel[Int])
      formula.connectFrom(grid)
      formula.outMetaData === Some(meta ~ int("z"))
      formula.output.collect.map(_.getInt("z")).toSet === Set(186, 2)
    }
    "process mixed field expressions" in {
      val formula = Formula("x" -> "$.a".json("data"), "y" -> "a".xpath("info"),
        "z" -> "count * weight".mvel[Int])
      formula.connectFrom(grid)
      formula.outMetaData === Some(meta ~ string("x") ~ string("y") ~ int("z"))
      val out = formula.output.collect
      out.map(_.getString("x")).toSet === Set("5", "1")
      out.map(_.getString("y")).toSet === Set("<a>X</a>", "<a>Y</a>")
      out.map(_.getInt("z")).toSet === Set(186, 2)
    }
    "save to xml" in {
      val formula = Formula("x" -> "$.a".json("data"), "y" -> "a".xpath("info"),
        "z" -> "count * weight".mvel[Int])
      <formula>
        <field name="x"><json src="data">$.a</json></field>
        <field name="y"><xpath src="info">a</xpath></field>
        <field name="z"><mvel type="int">count * weight</mvel></field>
      </formula> must ==/(formula.toXml)
    }
    "load from xml" in {
      Formula.fromXml(
        <formula>
          <field name="x"><json src="data">$.a</json></field>
          <field name="y"><xpath src="info">a</xpath></field>
          <field name="z"><mvel type="int">count * weight</mvel></field>
        </formula>) === Formula("x" -> "$.a".json("data"), "y" -> "a".xpath("info"),
          "z" -> "count * weight".mvel[Int])
    }
  }
}