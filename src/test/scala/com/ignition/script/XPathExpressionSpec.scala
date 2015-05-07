package com.ignition.script

import org.apache.spark.sql.Row
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import com.ignition.types._

@RunWith(classOf[JUnitRunner])
class XPathExpressionSpec extends Specification {

  val schema = string("payload").schema
  val row = Row(
    <items>
      <item>
        <name>John</name>
      </item>
      <item>
        <name>Jack</name>
      </item>
    </items>.toString)

  "xpath expressions" should {
    "evaluate against string data" in {
      val proc = "name".xpath("payload")
      proc.evaluate(schema)(row) === "<name>John</name><name>Jack</name>"
    }
  }
}