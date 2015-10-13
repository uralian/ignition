package com.ignition.stream

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

import com.ignition.types.{ fieldToRichStruct, int, string }

@RunWith(classOf[JUnitRunner])
class QueueInputSpec extends StreamFlowSpecification {

  val schema = string("name") ~ int("age")

  "QueueInput" should {
    "generate data stream" in {
      val queue = QueueInput(schema).
        addRows(("john", 10), ("jane", 15), ("jake", 18)).
        addRows(("john", 20), ("jane", 25), ("jake", 28)).
        addRows(("john", 30), ("jane", 35), ("jake", 38))

      runAndAssertOutput(queue, 0, 3,
        Set(("john", 10), ("jane", 15), ("jake", 18)),
        Set(("john", 20), ("jane", 25), ("jake", 28)),
        Set(("john", 30), ("jane", 35), ("jake", 38)))
    }
    "save to/load from xml" in {
      val queue = QueueInput(schema).
        addRows(("john", 10), ("jane", 15), ("jake", 18)).
        addRows(("john", 20), ("jane", 25), ("jake", 28)).
        addRows(("john", 30), ("jane", 35), ("jake", 38))
      queue.toXml must ==/(
        <stream-queue-input>
          <schema>
            <field name="name" type="string" nullable="true"/>
            <field name="age" type="integer" nullable="true"/>
          </schema>
          <data>
            <batch>
              <row><item>john</item><item>10</item></row>
              <row><item>jane</item><item>15</item></row>
              <row><item>jake</item><item>18</item></row>
            </batch>
            <batch>
              <row><item>john</item><item>20</item></row>
              <row><item>jane</item><item>25</item></row>
              <row><item>jake</item><item>28</item></row>
            </batch>
            <batch>
              <row><item>john</item><item>30</item></row>
              <row><item>jane</item><item>35</item></row>
              <row><item>jake</item><item>38</item></row>
            </batch>
          </data>
        </stream-queue-input>)
      QueueInput.fromXml(queue.toXml) === queue
    }
    "save to/load from json" in {
      val queue = QueueInput(schema).
        addRows(("john", 10), ("jane", 15), ("jake", 18)).
        addRows(("john", 20), ("jane", 25), ("jake", 28)).
        addRows(("john", 30), ("jane", 35), ("jake", 38))
      val queue2 = QueueInput.fromJson(queue.toJson)
      QueueInput.fromJson(queue.toJson) === queue
    }
  }
}