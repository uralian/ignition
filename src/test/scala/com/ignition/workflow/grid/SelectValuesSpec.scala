package com.ignition.workflow.grid

import org.joda.time.DateTime
import org.junit.runner.RunWith
import org.specs2.matcher.XmlMatchers
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import com.eaio.uuid.UUID
import com.ignition.SparkTestHelper
import com.ignition.data.{ Binary, DataType }
import com.ignition.data.{ Decimal, DefaultDataRow, DefaultRowMetaData, RowMetaData, TypeConversionException, boolean, columnInfo2metaData, decimal, int, string, string2richList }
import com.ignition.data.DataType.{ BinaryDataType, BooleanDataType, DateTimeDataType, DecimalDataType, DoubleDataType, IntDataType, StringDataType, UUIDDataType }
import com.ignition.workflow.rdd.grid.SelectAction
import com.ignition.workflow.rdd.grid.SelectAction.{ Remove, Rename, Retain, Retype }
import com.ignition.workflow.rdd.grid.SelectValues
import com.ignition.workflow.rdd.grid.input.DataGridInput

@RunWith(classOf[JUnitRunner])
class SelectValuesSpec extends Specification with XmlMatchers with SparkTestHelper {

  val meta = string("a") ~ int("b") ~ boolean("c")
  val row = meta.row("25.2", 10, true)

  "SelectAction" should {
    "load Retain action from xml" in {
      SelectAction.fromXml(<retain><col name="a"/><col name="b"/></retain>) === Retain("a", "b")
    }
    "load Rename action from xml" in {
      SelectAction.fromXml(<rename><col old="a" new="x"/><col old="b" new="y"/></rename>) === Rename("a" -> "x", "b" -> "y")
    }
    "load Remove action from xml" in {
      SelectAction.fromXml(<remove><col name="a"/><col name="b"/></remove>) === Remove("a", "b")
    }
    "load Retype action from xml" in {
      SelectAction.fromXml(<retype>
                             <col name="a" type="binary"/><col name="b" type="boolean"/>
                           </retype>) ===
        Retype("a" -> implicitly[DataType[Binary]], "b" -> implicitly[DataType[Boolean]])
    }
  }

  "Retain" should {
    "retain existing columns" in {
      val action = Retain("a", "c")
      action(row) === ("a" ~ "c").row(row.rawData.patch(1, Nil, 1))
      action(meta) === string("a") ~ boolean("c")
    }
    "fail for non-existing columns" in {
      val action = Retain("a", "b", "x")
      action(row) must throwA[RuntimeException]
      action(meta) must throwA[RuntimeException]
    }
    "save to xml" in {
      <retain><col name="a"/><col name="b"/></retain> must ==/(Retain("a", "b").toXml)
    }
  }

  "Rename" should {
    "rename existing columns" in {
      val action = Rename("b" -> "x", "a" -> "y")
      action(row) === ("y" ~ "x" ~ "c").row(row.rawData)
      action(meta) === string("y") ~ int("x") ~ boolean("c")
    }
    "skip non-existing columns" in {
      val action = Rename("z" -> "a")
      action(row) === row
      action(meta) === meta
    }
    "fail for duplicate columns" in {
      val action = Rename("a" -> "b")
      action(row) must throwA[RuntimeException]
      action(meta) must throwA[RuntimeException]
    }
    "save to xml" in {
      <rename><col old="a" new="x"/><col old="c" new="y"/></rename> must ==/(Rename("a" -> "x", "c" -> "y").toXml)
    }
  }

  "Remove" should {
    "remove existing columns" in {
      val action = Remove("b", "c")
      action(row) === "a".row(row.rawData.patch(1, Nil, 2))
      action(meta) === DefaultRowMetaData().add[String]("a")
    }
    "skip non-existing columns" in {
      val action = Remove("z")
      action(row) === row
      action(meta) === meta
    }
    "save to xml" in {
      <remove><col name="a"/><col name="b"/></remove> must ==/(Remove("a", "b").toXml)
    }
  }

  "Retype" should {
    "convert Boolean to String" in {
      val action = Retype("c" -> implicitly[DataType[String]])
      action(row) === ("a" ~ "b" ~ "c").row(row.rawData.patch(2, List(row.getString(2)), 1))
      val meta2 = action(meta)
      meta2.columnCount === 3
      meta2.columns(0).name === "a"
      meta2.columns(0).dataType === implicitly[DataType[String]]
      meta2.columns(1).name === "b"
      meta2.columns(1).dataType === implicitly[DataType[Int]]
      meta2.columns(2).name === "c"
      meta2.columns(2).dataType === implicitly[DataType[String]]
    }
    "convert String to Double" in {
      val action = Retype("a" -> implicitly[DataType[Double]])
      action(row) === ("a" ~ "b" ~ "c").row(row.rawData.patch(0, List(row.getDouble(0)), 1))
      val meta2 = action(meta)
      meta2.columnCount === 3
      meta2.columns(0).name === "a"
      meta2.columns(0).dataType === implicitly[DataType[Double]]
      meta2.columns(1).name === "b"
      meta2.columns(1).dataType === implicitly[DataType[Int]]
      meta2.columns(2).name === "c"
      meta2.columns(2).dataType === implicitly[DataType[Boolean]]
    }
    "convert Int to Decimal" in {
      val action = Retype("b" -> implicitly[DataType[Decimal]])
      action(row) === ("a" ~ "b" ~ "c").row(row.rawData.patch(1, List(row.getDecimal(1)), 1))
      val meta2 = action(meta)
      meta2.columnCount === 3
      meta2.columns(0).name === "a"
      meta2.columns(0).dataType === implicitly[DataType[String]]
      meta2.columns(1).name === "b"
      meta2.columns(1).dataType === implicitly[DataType[Decimal]]
      meta2.columns(2).name === "c"
      meta2.columns(2).dataType === implicitly[DataType[Boolean]]
    }
    "fail for incompatible conversion" in {
      val action = Retype("c" -> implicitly[DataType[UUID]])
      action(row) must throwA[TypeConversionException]
    }
    "save to xml" in {
      <retype>
        <col name="a" type="decimal"/><col name="c" type="string"/>
      </retype> must ==/(Retype("a" -> implicitly[DataType[Decimal]],
        "c" -> implicitly[DataType[String]]).toXml)
    }
  }

  "SelectValues" should {
    "process action chain" in {
      val grid = DataGridInput(meta, Seq(row))
      val select = SelectValues.create.
        rename("a" -> "x").retype[Decimal]("x").remove("b", "c")
      grid.connectTo(select)
      select.outMetaData === Some(decimal("x"): RowMetaData)
      select.output.collect.toSet === Set(DefaultDataRow(Vector("x"), Vector(BigDecimal("25.2"))))
    }
    "save to xml" in {
      val step = SelectValues(Rename("a" -> "x"),
        Retype("x" -> implicitly[DataType[DateTime]]), Remove("b", "c"))
      <select-values>
        <rename><col old="a" new="x"/></rename>
        <retype><col name="x" type="datetime"/></retype>
        <remove><col name="b"/><col name="c"/></remove>
      </select-values> must ==/(step.toXml)
    }
  }
}