package com.ignition.frame

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

import com.ignition.types._

@RunWith(classOf[JUnitRunner])
class AddFieldsSpec extends FrameFlowSpecification {

  val grid = DataGrid(string("name")) rows (Tuple1("john"), Tuple1("jane"))

  "AddFields" should {
    "add numeric columns" in {
      val af = AddFields("a" -> 5, "b" -> 2.toByte) % ("c" -> 9L) % ("d" -> 4.toShort) %
        ("e" -> 2.5) % ("f" -> 3f)
      grid --> af

      assertSchema(string("name") ~ int("a", false) ~ byte("b", false) ~ long("c", false) ~
        short("d", false) ~ double("e", false) ~ float("f", false), af, 0)
      assertOutput(af, 0, ("john", 5, 2, 9, 4, 2.5, 3), ("jane", 5, 2, 9, 4, 2.5, 3))
    }
    "add string columns" in {
      val af = AddFields("a" -> "abc")
      grid --> af

      assertSchema(string("name") ~ string("a", false), af, 0)
      assertOutput(af, 0, ("john", "abc"), ("jane", "abc"))
    }
    "add boolean columns" in {
      val af = AddFields("a" -> true, "b" -> false)
      grid --> af

      assertSchema(string("name") ~ boolean("a", false) ~ boolean("b", false), af, 0)
      assertOutput(af, 0, ("john", true, false), ("jane", true, false))
    }
    "add date columns" in {
      val d = javaDate(2015, 6, 15)
      val t = javaTime(2015, 3, 4, 5, 15)
      val af = AddFields("a" -> d, "b" -> t)
      grid --> af

      assertSchema(string("name") ~ date("a", false) ~ timestamp("b", false), af, 0)
      assertOutput(af, 0, ("john", d, t), ("jane", d, t))
    }
    "add binary colunms" in {
      val array = Array(1.toByte, 2.toByte, 3.toByte)
      val af = AddFields("a" -> array)
      grid --> af

      assertSchema(string("name") ~ binary("a", false), af, 0)
      val rows = af.output.collect
      rows.size === 2
      rows(0).get(1) === array
      rows(1).get(1) === array
    }
    "add variable columns" in {
      System.setProperty("a", "AAA")
      val sv = SetVariables("b" -> 123, "c" -> true, "d" -> "hello")
      val af = AddFields("a" -> e"a", "b" -> v"b", "c" -> v"c", "d" -> v"d")
      grid --> sv --> af

      assertSchema(string("name") ~ string("a", false) ~ int("b", false) ~ boolean("c", false) ~
        string("d", false), af, 0)
      assertOutput(af, 0, ("john", "AAA", 123, true, "hello"), ("jane", "AAA", 123, true, "hello"))
    }
    "save to/load from xml" in {
      val af = AddFields("a" -> 5, "b" -> 2.toByte, "c" -> 9L, "d" -> 4.toShort, "e" -> 2.5, "f" -> 3f,
        "g" -> "abc", "h" -> false, "i" -> javaDate(2015, 6, 15), "j" -> javaTime(2015, 3, 4, 5, 15),
        "k" -> v"XYZ", "l" -> e"Prop")

      af.toXml must ==/(
        <add-fields>
          <field name="a" type="integer">5</field>
          <field name="b" type="byte">2</field>
          <field name="c" type="long">9</field>
          <field name="d" type="short">4</field>
          <field name="e" type="double">2.5</field>
          <field name="f" type="float">3.0</field>
          <field name="g" type="string">abc</field>
          <field name="h" type="boolean">false</field>
          <field name="i" type="date">2015-06-15</field>
          <field name="j" type="timestamp">{ TypeUtils.formatTimestamp(javaTime(2015, 3, 4, 5, 15)) }</field>
          <field name="k" type="var">XYZ</field>
          <field name="l" type="env">Prop</field>
        </add-fields>)
      AddFields.fromXml(af.toXml) === af
    }
    "save to/load from json" in {
      import org.json4s.JsonDSL._

      val af = AddFields("a" -> 5, "b" -> 2.toByte, "c" -> 9L, "d" -> 4.toShort, "e" -> 2.5, "f" -> 3f,
        "g" -> "abc", "h" -> false, "i" -> javaDate(2015, 6, 15), "j" -> javaTime(2015, 3, 4, 5, 15),
        "k" -> v"XYZ", "l" -> e"Prop")

      af.toJson === ("tag" -> "add-fields") ~ ("fields" -> List(
        ("name" -> "a") ~ ("type" -> "integer") ~ ("value" -> 5),
        ("name" -> "b") ~ ("type" -> "byte") ~ ("value" -> 2),
        ("name" -> "c") ~ ("type" -> "long") ~ ("value" -> 9),
        ("name" -> "d") ~ ("type" -> "short") ~ ("value" -> 4),
        ("name" -> "e") ~ ("type" -> "double") ~ ("value" -> 2.5),
        ("name" -> "f") ~ ("type" -> "float") ~ ("value" -> 3.0),
        ("name" -> "g") ~ ("type" -> "string") ~ ("value" -> "abc"),
        ("name" -> "h") ~ ("type" -> "boolean") ~ ("value" -> false),
        ("name" -> "i") ~ ("type" -> "date") ~ ("value" -> "2015-06-15"),
        ("name" -> "j") ~ ("type" -> "timestamp") ~ ("value" -> TypeUtils.formatTimestamp(javaTime(2015, 3, 4, 5, 15))),
        ("name" -> "k") ~ ("type" -> "var") ~ ("value" -> "XYZ"),
        ("name" -> "l") ~ ("type" -> "env") ~ ("value" -> "Prop")))
      AddFields.fromJson(af.toJson) === af
    }
  }
}