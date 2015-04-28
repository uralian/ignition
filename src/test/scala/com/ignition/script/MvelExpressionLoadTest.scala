package com.ignition.script

import scala.util.Random
import org.apache.spark.sql.types.Decimal
import org.apache.spark.sql.Row
import com.ignition.types._
import org.joda.time.DateTime
import com.eaio.uuid.UUID
import com.ignition.TestDataHelper

object MvelExpressionLoadTest extends App with TestDataHelper {
  import com.ignition.script.ScriptFunctions._

  val schema = string("customer_id") ~ string("description") ~ date("date") ~
    decimal("total") ~ int("items") ~ double("weight") ~ boolean("shipped")
  val row = Row(new UUID().toString, "goods", javaDate(2015, 2, 24),
    javaBD("125.99"), 3, 62.15, true)

  testExpression("FACTORIAL(items) * POW(weight, 2) + FLOOR(weight) * items * 25.5")
  referenceTest()
  testExpression("items + weight * items + ABS(total) * 5")
  testExpression("LOWER(description + items * weight + '_' + total)")
  testExpression("FORMAT('%s - %d - %.2f - %s', description, items, total * weight, date)")

  def referenceTest(count: Int = 100000): Unit = {
    val rows = (1 to count) map createRow

    val start = System.currentTimeMillis
    val dummy = for { r <- rows } yield {
      val data = row2javaMap(schema)(r)
      val items = data.get("items").asInstanceOf[Int]
      val weight = data.get("weight").asInstanceOf[Double]
      ScriptFunctions.FACTORIAL(items) * ScriptFunctions.POW(weight, 2) + weight * items * 25.5
    }
    val end = System.currentTimeMillis
    println(s"Reference Scala test evaluated $count times in ${end - start} ms")
  }

  def testExpression(expr: String, count: Int = 100000): Unit = {
    val proc = expr.mvel
    val execute = proc.evaluate(schema) _
    val result = execute(row)

    println(expr)
    println(s"\t = $result (${result.getClass})")

    val rows = (1 to count) map createRow

    val start = System.currentTimeMillis
    val dummy = for { r <- rows } yield execute(r).hashCode
    val end = System.currentTimeMillis
    println(s"\tevaluated $count times in ${end - start} ms")
  }

  protected def createRow(seed: Int) = Row(new UUID().toString, Random.nextString(5),
    DateTime.now: java.sql.Date,
    Decimal(Random.nextInt(100000) / 1000.0 - 50).toJavaBigDecimal,
    Random.nextInt(100) - 30, Random.nextInt(100000) / 5000.0, Random.nextBoolean)
}