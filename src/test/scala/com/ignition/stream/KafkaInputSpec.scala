package com.ignition.stream

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import com.ignition.types.{ fieldToRichStruct, int, string }
import com.ignition.DefaultSparkRuntime
import org.apache.spark.streaming.ClockWrapper

@RunWith(classOf[JUnitRunner])
class KafkaInputSpec extends StreamFlowSpecification {

  "KafkaInput" should {
    "work" in {
      //TODO need to mock up kafka broker here
      success
    }
  }

}