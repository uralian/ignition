package com.ignition.samples

import com.eaio.uuid.UUID

import com.ignition.SparkPlug
import com.ignition.flow._
import com.ignition.types._
import com.ignition.flow._

object UnionFlow extends App {

  val flow = DataFlow {
    val schema = string("id") ~ string("name") ~ int("weight") ~ date("dob")

    val grid1 = DataGrid(schema).
      addRow(newid, "john", 155, dob(1980, 5, 2)).
      addRow(newid, "jake", 160, dob(1974, 11, 3)).
      addRow(newid, "josh", 120, dob(1995, 1, 10))

    val grid2 = DataGrid(schema).
      addRow(newid, "jane", 190, dob(1982, 4, 25)).
      addRow(newid, "jake", 160, dob(1974, 11, 3)).
      addRow(newid, "jill", 120, dob(1995, 1, 10))

    val grid3 = DataGrid(schema).
      addRow(newid, "jess", 155, dob(1980, 5, 2))

    val union = Union()

    val debug = DebugOutput()

    val csv = TextFileOutput("union_output.csv", "name" -> "%-10s", "weight" -> "%6d", "dob" -> "%s")
    
    (grid1, grid2, grid3) --> union --> debug
    union --> csv
    
    (debug, csv)
  }
  
  SparkPlug.runDataFlow(flow)

  private def newid = new UUID().toString

  private def dob(year: Int, month: Int, day: Int) = java.sql.Date.valueOf(s"$year-$month-$day")
}