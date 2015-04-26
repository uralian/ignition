package com.ignition.flow

import org.junit.runner.RunWith
import org.specs2.matcher.XmlMatchers
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import com.ignition.SparkTestHelper

@RunWith(classOf[JUnitRunner])
class DebugOutputSpec extends FlowSpecification {
  "DebugOutput" should {
    "save to xml for default values" in {
      val debug = DebugOutput()
      <debug-output names="true" types="false"/> must ==/(debug.toXml)
    }
    "save to xml for custom values" in {
      val debug = DebugOutput(false, true)
      <debug-output names="false" types="true" /> must ==/(debug.toXml)
    }
    "load from xml" in {
      val xml = <debug-output names="false" types="true" />
      DebugOutput.fromXml(xml) === DebugOutput(false, true)
    }
  }
}