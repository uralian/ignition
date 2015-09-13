package com.ignition.stream

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

import com.ignition.RichProduct
import com.ignition.types._

@RunWith(classOf[JUnitRunner])
class JoinSpec extends StreamFlowSpecification {
  sequential

  val schema1 = string("name") ~ int("item") ~ double("score")
  val queue1 = QueueInput(schema1).
    addRows(("john", 1, 15.0), ("john", 3, 10.0)).
    addRows(("jake", 4, 25.0), ("john", 3, 30.0))

  val schema2 = string("name") ~ int("age") ~ boolean("flag")
  val queue2 = QueueInput(schema1).
    addRows(("jake", 20, true), ("john", 15, false)).
    addRows(("jane", 10, true), ("john", 30, true))

  "Join without condition" should {
    "produce Cartesian project" in {
      val join = Join()
      (queue1, queue2) --> join

      runAndAssertOutput(join, 0, 2,
        Set(("john", 1, 15.0, "jake", 20, true), ("john", 3, 10.0, "jake", 20, true),
          ("john", 1, 15.0, "john", 15, false), ("john", 3, 10.0, "john", 15, false)),
        Set(("jake", 4, 25.0, "jane", 10, true), ("john", 3, 30.0, "jane", 10, true),
          ("jake", 4, 25.0, "john", 30, true), ("john", 3, 30.0, "john", 30, true)))
    }
  }
}