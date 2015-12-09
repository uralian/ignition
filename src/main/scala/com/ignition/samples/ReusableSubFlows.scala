package com.ignition.samples

import org.apache.spark.sql.types.Decimal

import com.ignition.{ CSource2, frame }
import com.ignition.frame._
import com.ignition.script.RichString
import com.ignition.types._

object ReusableSubFlows extends App {

  // schemas
  val abSchema = int("a") ~ int("b")
  val customerSchema = string("name") ~ boolean("local") ~ double("cost")
  val orderSchema = date("order_date") ~ decimal("amount") ~ string("name")

  // subflow0 | data provider: no inputs, three outputs
  val provider = FrameSubModule {
    val abGrid = DataGrid(abSchema) rows ((2, 3), (4, 2), (2, 2), (3, 1))
    val customerGrid = DataGrid(customerSchema) rows (
      ("john", true, 25.36), ("jack", false, 74.15), ("jane", true, 19.99))
    val orderGrid = DataGrid(orderSchema) rows (
      (javaDate(2010, 1, 3), Decimal(120.55), "john"),
      (javaDate(2010, 3, 10), Decimal(42.85), "jack"),
      (javaDate(2010, 2, 9), Decimal(44.17), "john"),
      (javaDate(2010, 5, 10), Decimal(66.99), "jane"),
      (javaDate(2010, 1, 3), Decimal(55.08), "john"))
    (Nil, Seq(abGrid, customerGrid, orderGrid))
  }

  // subflow1 | simple multiplier: one input - one output
  val multiplier = FrameSubTransformer {
    val formula = Formula("ab" -> "a * b".mvel)
    val select = SelectValues() rename ("ab" -> "total") retype ("total" -> "double")
    formula --> select
    (formula, select)
  }

  // subflow2 | joiner: two inputs - one output
  val joiner = FrameSubMerger {
    val query = SQLQuery("""
            SELECT o.order_date, o.amount, c.name, c.cost, o.amount + c.cost AS total
            FROM input0 c
            JOIN input1 o
            ON o.name = c.name
            ORDER BY c.name, total""")
    (query.in, query)
  }

  // main data flow | uses all subflows defined above
  val flow = DataFlow {
    val abDebug = DebugOutput()
    val jDebug0 = DebugOutput()
    val jDebug1 = DebugOutput()
    val jDebug2 = DebugOutput()

    provider.out(0) --> multiplier --> abDebug
    (provider.out(1), provider.out(2)) --> joiner --> (jDebug0, jDebug1, jDebug2)

    (abDebug, jDebug0, jDebug1, jDebug2)
  }

  frame.Main.runDataFlow(flow)
}