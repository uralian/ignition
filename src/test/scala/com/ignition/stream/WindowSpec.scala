package com.ignition.stream

import org.apache.spark.streaming.Milliseconds
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

import com.ignition.ExecutionException
import com.ignition.types.{ RichStructType, double, fieldToRichStruct, int, string }

@RunWith(classOf[JUnitRunner])
class WindowSpec extends StreamFlowSpecification {
  sequential

  val schema = string("name") ~ int("item") ~ double("score")

  val queue = QueueInput(schema).
    addRows(("john", 1, 65.0), ("john", 3, 78.0)).
    addRows(("jane", 2, 85.0), ("jane", 1, 46.0)).
    addRows(("jake", 4, 62.0), ("john", 3, 95.0))

  val step = batchDuration 

  "Window" should {
    "create sliding windows" in {
      val w = Window(step * 2)
      queue --> w

      runAndAssertOutput(w, 0, 5,
        Set(("john", 1, 65.0), ("john", 3, 78.0)),
        Set(("john", 1, 65.0), ("john", 3, 78.0), ("jane", 2, 85.0), ("jane", 1, 46.0)),
        Set(("jane", 2, 85.0), ("jane", 1, 46.0), ("jake", 4, 62.0), ("john", 3, 95.0)),
        Set(("jake", 4, 62.0), ("john", 3, 95.0)),
        Set())
    }
    "save to/load from xml" in {
      val w = Window(step * 2, step)
      w.toXml must ==/(<stream-window duration="200" slide="100" />)
      Window.fromXml(w.toXml) === w
    }
    "save to/load from json" in {
      import org.json4s.JsonDSL._
      
      val w = Window(step * 2, step)
      w.toJson === ("tag" -> "stream-window") ~ ("duration" -> 200) ~ ("slide" -> 100)
      Window.fromJson(w.toJson) === w
    }
  }
}