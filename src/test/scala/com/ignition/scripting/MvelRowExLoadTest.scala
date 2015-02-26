package com.ignition.scripting

import scala.util.Random

import org.joda.time.DateTime

import com.eaio.uuid.UUID
import com.ignition.data.{ RichBoolean, boolean, columnInfo2metaData, datetime, decimal, double, int, string, uuid }
import com.ignition.data.DataType.StringDataType

object MvelRowExLoadTest extends App {

  val meta = uuid("customer_id") ~ string("description") ~ datetime("date") ~
    decimal("total") ~ int("items") ~ double("weight") ~ boolean("shipped")
  val row = meta.row(new UUID, "goods", DateTime.now, BigDecimal("125.99"), 3, 62.15, true)

  testExpression("FACTORIAL(items) * POW(weight, 2) + FLOOR(weight) * items / 25.5")
  referenceTest()
  testExpression("items + weight * items + ABS(total) * 5")
  testExpression("LOWER(description + items * weight + '_' + total)")
  testExpression("FORMAT('%s - %d - %.2f - %s', description, items, total * weight, date)")

  def referenceTest(count: Int = 100000): Unit = {
    val rows = (1 to count) map { _ =>
      meta.row(new UUID, Random.nextString(5), DateTime.now,
        BigDecimal(Random.nextInt(100000) / 1000.0 - 50), Random.nextInt(100) - 30,
        Random.nextDouble * 1000, Random.nextBoolean)
    }

    val start = System.currentTimeMillis
    val dummy = for { r <- rows } yield {
      val items = r.getInt("items")
      val weight = r.getDouble("weight")
      ScriptFunctions.FACTORIAL(items) * ScriptFunctions.POW(weight, 5) + weight * items / 25.5
    }
    val end = System.currentTimeMillis
    println(s"Reference Scala test evaluated $count times in ${end - start} ms")
  }

  def testExpression(expr: String, count: Int = 10): Unit = {
    testExpression(expr, false, count)
    testExpression(expr, true, count)
  }

  def testExpression(expr: String, strict: Boolean, count: Int): Unit = {
    val mode = strict ? ("STRICT", "NONSTRICT")
    println(s"$mode: $expr")

    val proc = new MvelRowExProcessor[String](expr, None)
    val result = proc.evaluate(strict ? (Some(meta), None))(row)

    println(s"\t= $result (${result.getClass})")

    val rows = (1 to count) map { _ =>
      meta.row(new UUID, Random.nextString(5), DateTime.now,
        BigDecimal(Random.nextInt(100000) / 1000.0 - 50), Random.nextInt(100) - 30,
        Random.nextDouble * 1000, Random.nextBoolean)
    }

    var start = System.currentTimeMillis
    val dummy = for { r <- rows } yield proc.evaluate(None)(r).hashCode
    var end = System.currentTimeMillis
    println(s"\tevaluated $count times in ${end - start} ms")
  }
}