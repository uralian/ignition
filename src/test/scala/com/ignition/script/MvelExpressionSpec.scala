package com.ignition.script

import org.apache.spark.sql.Row
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import com.ignition.types._
import org.apache.spark.sql.types.Decimal
import com.ignition.TestDataHelper

@RunWith(classOf[JUnitRunner])
class MvelExpressionSpec extends Specification with TestDataHelper {

  val schema = string("customer_id") ~ string("description") ~ date("date") ~
    decimal("total") ~ int("items") ~ double("weight") ~ boolean("shipped")
  val row = Row(java.util.UUID.randomUUID.toString, "goods", javaDate(2015, 2, 24),
    javaBD("125.99"), 3, 62.15, true)

  "mvel numeric expressions" should {
    "work in strict mode" in {
      val proc = "items * 5 + weight - total".mvel
      proc.evaluate(schema)(row).asInstanceOf[Number].doubleValue must be ~ (-48.84 +/- 0.5)
    }
  }

  "mvel numeric expression with functions" should {
    "work in strict mode" in {
      val proc = "FLOOR(weight) * POW(items, 2)".mvel
      proc.evaluate(schema)(row) === 558
    }
  }

  "mvel string expressions" should {
    "work in strict mode" in {
      val proc = "description + UPPER(description)".mvel
      proc.evaluate(schema)(row) === "goodsGOODS"
    }
  }

  "mvel date expressions" should {
    "work in strict mode" in {
      val proc = "DAY(PLUS(date, 4, 'day'))".mvel
      proc.evaluate(schema)(row) === 28
    }
  }
}