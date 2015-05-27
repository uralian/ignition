package com.ignition.frame

import org.apache.spark.sql.Row
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

import com.ignition.TestDataHelper
import com.ignition.script.RichString
import com.ignition.types.{ RichStructType, date, double, fieldToRichStruct, string }

@RunWith(classOf[JUnitRunner])
class FormulaSpec extends FrameFlowSpecification with TestDataHelper {

  val schema = string("name") ~ date("dob") ~ double("total") ~ string("xml") ~ string("json")
  val row0 = Row("john", javaDate(1990, 12, 5), 123.45, <a><b>123</b></a>.toString, """{"books" : ["a", "b", "c"]}""")
  val row1 = Row("jane", javaDate(1975, 2, 12), 25.1, <a><b>45</b></a>.toString, """{"books" : ["x"]}""")
  val row2 = Row("jack", javaDate(1984, 4, 21), 349.0, <a><b>67</b></a>.toString, """{"books" : []}""")
  val row3 = Row("jake", javaDate(1999, 11, 25), 4.22, <a><c></c></a>.toString, """{"movies" : ["a", "b", "c"]}""")
  val row4 = Row("jill", javaDate(1970, 7, 2), 77.13, <a></a>.toString, """{"books" : ["c", "c", "c"]}""")
  val row5 = Row("josh", javaDate(1981, 7, 18), 13.6, <a><b></b></a>.toString, """{"books" : ["1", "2"]}""")
  val row6 = Row("judd", javaDate(1994, 2, 20), 5.999, <a><b>x</b></a>.toString, """{}""")
  val row7 = Row("jess", javaDate(1974, 1, 27), 15.0, <b>...</b>.toString, """{"books" : "some"}""")
  val grid = DataGrid(schema, Seq(row0, row1, row2, row3, row4, row5, row6, row7))

  "Formula" should {
    "work for XPath expressions" in {
      val formula = Formula("X" -> "b".xpath("xml"))
      grid --> formula
      assertSchema(schema ~ string("X"), formula, 0)
      assertOutput(formula, 0,
        apd(row0, "<b>123</b>"), apd(row1, "<b>45</b>"), apd(row2, "<b>67</b>"), apd(row3, ""),
        apd(row4, ""), apd(row5, "<b/>"), apd(row6, "<b>x</b>"), apd(row7, "<b>...</b>"))
    }
    "work for JsonPath expressions" in {
      val formula = Formula("Y" -> "$.books[1]".json("json"))
      grid --> formula
      assertSchema(schema ~ string("Y"), formula, 0)
      assertOutput(formula, 0,
        apd(row0, "b"), apd(row1, ""), apd(row2, ""), apd(row3, ""),
        apd(row4, "c"), apd(row5, "2"), apd(row6, ""), apd(row7, ""))
    }
    "work for Mvel expressions" in {
      val formula = Formula("Z" -> "YEAR(dob) + total / 2".mvel)
      grid --> formula
      assertSchema(schema ~ double("Z"), formula, 0)
      assertOutput(formula, 0,
        apd(row0, 2051.725), apd(row1, 1987.55), apd(row2, 2158.5), apd(row3, 2001.11),
        apd(row4, 2008.565), apd(row5, 1987.8), apd(row6, 1996.9995), apd(row7, 1981.5))
    }
    "work for multiple expressions" in {
      val formula = Formula("X" -> "b".xpath("xml"), "Y" -> "$.books[1]".json("json"),
        "Z" -> "YEAR(dob) + total / 2".mvel)
      grid --> formula
      assertSchema(schema ~ string("X") ~ string("Y") ~ double("Z"), formula, 0)
      assertOutput(formula, 0,
        apd(row0, "<b>123</b>", "b", 2051.725), apd(row1, "<b>45</b>", "", 1987.55),
        apd(row2, "<b>67</b>", "", 2158.5), apd(row3, "", "", 2001.11),
        apd(row4, "", "c", 2008.565), apd(row5, "<b/>", "2", 1987.8),
        apd(row6, "<b>x</b>", "", 1996.9995), apd(row7, "<b>...</b>", "", 1981.5))
    }
    "be unserializable" in assertUnserializable(Formula("Y" -> "$.books[1]".json("json")))
  }

  private def apd(row: Row, values: Any*) = Row(row.toSeq ++ values: _*)
}