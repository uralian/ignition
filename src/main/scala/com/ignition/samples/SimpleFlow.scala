package com.ignition.samples

import com.eaio.uuid.UUID
import com.ignition.SparkPlug
import com.ignition.flow.{ DataFlow, DataGrid, DebugOutput, SQLQuery }
import com.ignition.types.{ RichStructType, date, fieldToStruct, int, string }

object SimpleFlow extends App {

  val flow = DataFlow {
    val grid = DataGrid(string("id") ~ string("name") ~ int("weight") ~ date("dob"))
      .addRow(newid, "john", 155, dob(1980, 5, 2))
      .addRow(newid, "jane", 190, dob(1982, 4, 25))
      .addRow(newid, "jake", 160, dob(1974, 11, 3))
      .addRow(newid, "josh", 120, dob(1995, 1, 10))

    val query = SQLQuery("""
      SELECT SUM(weight) AS total, AVG(weight) AS mean, MIN(weight) AS low
      FROM input0
      WHERE name LIKE 'j%'""")

    val debug = DebugOutput()

    grid --> query --> debug
  }

  SparkPlug.runDataFlow(flow)

  private def newid = new UUID().toString
  
  private def dob(year: Int, month: Int, day: Int) = java.sql.Date.valueOf(s"$year-$month-$day")
}