package com.ignition.frame

import org.apache.spark.sql.Row
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

import com.ignition.frame.SelectAction.{Remove, Rename, Retain, Retype}
import com.ignition.types._

@RunWith(classOf[JUnitRunner])
class SelectValuesSpec extends FrameFlowSpecification {

  val schema = string("a") ~ int("b") ~ boolean("c")
  val row = Row("25.2", 10, true)
  val grid = DataGrid(schema).addRow(row)
  val df = grid.output

  "Retain" should {
    "retain existing columns" in {
      val action = Retain("a", "c")
      assertDataFrame(action(df), Row("25.2", true))
      action(schema) === string("a") ~ boolean("c")
    }
    "fail for non-existing columns" in {
      val action = Retain("a", "b", "x")
      action(df) must throwA[Exception]
      action(schema) must throwA[Exception]
    }
  }

  "Rename" should {
    "rename existing columns" in {
      val action = Rename("b" -> "x", "a" -> "y")
      assertDataFrame(action(df), row)
      action(schema) === string("y") ~ int("x") ~ boolean("c")
    }
    "skip non-existing columns" in {
      val action = Rename("z" -> "a")
      assertDataFrame(action(df), row)
      action(schema) === schema
    }
    "fail for duplicate columns" in {
      val action = Rename("a" -> "b")
      action(df) must throwA[Throwable]
      action(schema) must throwA[Throwable]
    }
  }

  "Remove" should {
    "remove existing columns" in {
      val action = Remove("b", "c")
      assertDataFrame(action(df), Row("25.2"))
      action(schema) === string("a").schema
    }
    "skip non-existing columns" in {
      val action = Remove("z")
      assertDataFrame(action(df), row)
      action(schema) === schema
    }
  }

  "Retype" should {
    "convert Boolean to String" in {
      val action = Retype("c" -> "string")
      assertDataFrame(action(df), Row("25.2", 10, "true"))
      action(schema) === string("a") ~ int("b") ~ string("c")
    }
    "convert String to Double" in {
      val action = Retype("a" -> "double")
      assertDataFrame(action(df), Row(25.2, 10, true))
      action(schema) === double("a") ~ int("b") ~ boolean("c")
    }
    "convert Int to Decimal" in {
      val action = Retype("b" -> "decimal")
      assertDataFrame(action(df), Row("25.2", javaBD(10), true))
      action(schema) === string("a") ~ decimal("b") ~ boolean("c")
    }
  }

  "SelectValues" should {
    "process action chain" in {
      val select = SelectValues().rename("a" -> "x").retype("x" -> "decimal").remove("b", "c")
      grid --> select
      assertOutput(select, 0, Seq(javaBD("25.2")))
      assertSchema(decimal("x").schema, select, 0)
    }
    "be unserializable" in assertUnserializable(SelectValues())
  }
}