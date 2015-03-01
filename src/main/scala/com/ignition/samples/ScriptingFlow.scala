package com.ignition.samples

import org.apache.spark.SparkContext
import org.slf4j.LoggerFactory

import com.ignition.data.{ columnInfo2metaData, decimal, double, int, string }
import com.ignition.scripting.RichString
import com.ignition.workflow.rdd.grid.{ Formula, SelectValues }
import com.ignition.workflow.rdd.grid.input.DataGridInput

object ScriptingFlow extends App {

  val log = LoggerFactory.getLogger(getClass)

  implicit val sc = new SparkContext("local[4]", "test")

  val meta = string("info") ~ string("data") ~ decimal("price") ~ int("count") ~ double("discount")
  val rows = (1 to 10) map (n =>
    meta.row(<all><item>{ n }</item></all>, s"""{"data": {"code": $n}}""", BigDecimal(n * 5 / 10.0), n, 1.0 / n))
  val grid = DataGridInput(meta, rows)

  val formula1 = Formula(
    "total" -> "price * count * (1 - discount)".mvel[BigDecimal],
    "item" -> "item".xpath("info"),
    "code" -> "$.data.code".json("data"))

  val formula2 = Formula("totalWithTax" -> "total * 1.1".mvel[BigDecimal])

  val select = SelectValues().retain("code", "totalWithTax")

  grid.connectTo(formula1).connectTo(formula2).connectTo(select)

  println("Output meta: " + select.outMetaData)
  println("Output data:")
  select.output foreach println

  sc.stop
}