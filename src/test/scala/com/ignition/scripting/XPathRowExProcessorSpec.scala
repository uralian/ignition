package com.ignition.scripting

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import com.ignition.data.{ columnInfo2metaData, string }

@RunWith(classOf[JUnitRunner])
class XPathRowExProcessorSpec extends Specification {

  val meta = string("payload")
  val row = meta.row(
    <items>
      <item>
        <name>John</name>
      </item>
      <item>
        <name>Jack</name>
      </item>
    </items>)

  "xpath expressions" should {
    "evaluate against string data" in {
      val proc = new XPathRowExProcessor("payload", "name")
      proc.evaluate(None)(row) === "<name>John</name><name>Jack</name>"
    }
  }
}