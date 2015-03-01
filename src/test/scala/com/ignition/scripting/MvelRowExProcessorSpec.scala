package com.ignition.scripting

import org.joda.time.DateTime
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import com.eaio.uuid.UUID
import com.ignition.data.{ boolean, columnInfo2metaData, datetime, decimal, double, int, string, uuid }
import com.ignition.data.DataType.{ DoubleDataType, IntDataType, StringDataType }

@RunWith(classOf[JUnitRunner])
class MvelRowExProcessorSpec extends Specification {
  sequential

  val meta = uuid("customer_id") ~ string("description") ~ datetime("date") ~
    decimal("total") ~ int("items") ~ double("weight") ~ boolean("shipped")
  val row = meta.row(new UUID, "goods", new DateTime(2015, 2, 24, 0, 0),
    BigDecimal("125.99").bigDecimal, 3, 62.15, true)

  "mvel numeric expressions" should {
    "work in strict mode" in {
      val proc = new MvelRowExProcessor[Double]("items * 5 + weight - total", Some(meta))
      proc.evaluate(None)(row) must be ~ (-48.0 +/- 1.0)
    }
    "work in non-strict mode" in {
      val proc = new MvelRowExProcessor[Double]("items * 5 + weight - total", None)
      proc.evaluate(None)(row) must be ~ (-48.0 +/- 1.0)
    }
  }

  "mvel numeric expression with functions" should {
    "work in strict mode" in {
      val proc = new MvelRowExProcessor[Int]("FLOOR(weight) * POW(items, 2)", Some(meta))
      proc.evaluate(None)(row) === 558
    }
    "work in non-strict mode" in {
      val proc = new MvelRowExProcessor[Int]("FLOOR(weight) * POW(items, 2)", None)
      proc.evaluate(None)(row) === 558
    }
  }

  "mvel date expressions" should {
    "work in strict mode" in {
      val proc = new MvelRowExProcessor[Int]("DAY(PLUS(date, 4, 'day'))", Some(meta))
      proc.evaluate(None)(row) === 28
    }
    "work in non-strict mode" in {
      val proc = new MvelRowExProcessor[Int]("DAY(PLUS(date, 4, 'day'))", None)
      proc.evaluate(None)(row) === 28
    }
  }

  "mvel string expressions" should {
    "work in strict mode" in {
      val proc = new MvelRowExProcessor[String]("description + UPPER(description)", Some(meta))
      proc.evaluate(None)(row) === "goodsGOODS"
    }
    "work in non-strict mode" in {
      val proc = new MvelRowExProcessor[String]("description + UPPER(description)", None)
      proc.evaluate(None)(row) === "goodsGOODS"
    }
  }
}