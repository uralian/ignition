package com.ignition.frame

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DebugOutputSpec extends FrameFlowSpecification {
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