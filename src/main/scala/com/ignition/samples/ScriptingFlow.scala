package com.ignition.samples

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.Decimal

import com.ignition.frame
import com.ignition.frame.{ FrameFlow, DataGrid, DebugOutput, Formula, SelectValues }
import com.ignition.script.RichString
import com.ignition.types.{ RichStructType, decimal, double, fieldToRichStruct, int, string }

object ScriptingFlow extends App {

  val flow = FrameFlow {
    val schema = string("info") ~ string("data") ~ decimal("price") ~ int("count") ~ double("discount")
    val rows = (1 to 10) map (n =>
      Row(
        s"<all><item>$n</item></all>",
        s"""{"data": {"code": $n}}""",
        Decimal(n * 5 / 10.0),
        n,
        1.0 / n))
    val grid = DataGrid(schema, rows)

    val formula1 = Formula(
      "total" -> "price * count * (1 - discount)".mvel,
      "item" -> "item".xpath("info"),
      "code" -> "$.data.code".json("data"))

    val formula2 = Formula("totalWithTax" -> "total * 1.1".mvel)

    val select = SelectValues() retain ("code", "totalWithTax")

    val debug = DebugOutput()

    grid --> formula1 --> formula2 --> select --> debug
  }

  frame.Main.runFrameFlow(flow)
}