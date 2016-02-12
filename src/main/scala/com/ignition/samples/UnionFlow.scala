package com.ignition.samples

import com.ignition.{ CSource3, frame }
import com.ignition.frame.{ FrameFlow, DataGrid, DebugOutput, TextFileOutput, Union }
import com.ignition.types.{ RichStructType, date, fieldToRichStruct, int, string }

object UnionFlow extends App {

  val flow = FrameFlow {
    val schema = string("id") ~ string("name") ~ int("weight") ~ date("dob")

    val grid1 = DataGrid(schema).
      addRow(newid, "john", 155, javaDate(1980, 5, 2)).
      addRow(newid, "jake", 160, javaDate(1974, 11, 3)).
      addRow(newid, "josh", 120, javaDate(1995, 1, 10))

    val grid2 = DataGrid(schema).
      addRow(newid, "jane", 190, javaDate(1982, 4, 25)).
      addRow(newid, "jake", 160, javaDate(1974, 11, 3)).
      addRow(newid, "jill", 120, javaDate(1995, 1, 10))

    val grid3 = DataGrid(schema).
      addRow(newid, "jess", 155, javaDate(1980, 5, 2))

    val union = Union()

    val debug = DebugOutput()

    val csv = TextFileOutput("/tmp/union_output.csv", "name" -> "%-10s", "weight" -> "%6d", "dob" -> "%s")

    (grid1, grid2, grid3) --> union --> debug

    union --> csv

    (debug, csv)
  }

  frame.Main.runFrameFlow(flow)
}