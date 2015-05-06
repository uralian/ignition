package com.ignition.samples

import com.eaio.uuid.UUID
import com.ignition.SparkPlug
import com.ignition.flow.{ DataFlow, DataGrid, DebugOutput, SQLQuery }
import com.ignition.types._

object SimpleFlow extends App {

  val flow = DataFlow {
    val grid1 = DataGrid(string("id") ~ string("name") ~ int("weight") ~ date("dob")) rows (
      (newid, "john", 155, dob(1980, 5, 2)),
      (newid, "jane", 190, dob(1982, 4, 25)),
      (newid, "jake", 160, dob(1974, 11, 3)),
      (newid, "josh", 120, dob(1995, 1, 10))
    )
    
    val grid2 = DataGrid(string("name")) rows ("jane", "josh")

    val query = SQLQuery("""
      SELECT SUM(weight) AS total, AVG(weight) AS mean, MIN(weight) AS low
      FROM input0 JOIN input1 ON input0.name = input1.name
      WHERE input0.name LIKE 'j%'""")

    val debug = DebugOutput()

    (grid1, grid2) --> query --> debug
  }

  SparkPlug.runDataFlow(flow)

  private def newid = new UUID().toString
  
  private def dob(year: Int, month: Int, day: Int) = java.sql.Date.valueOf(s"$year-$month-$day")
}