//package com.ignition.stream
//
//import org.junit.runner.RunWith
//import org.specs2.runner.JUnitRunner
//
//import com.ignition.types.{ fieldToRichStruct, int, string }
//
//@RunWith(classOf[JUnitRunner])
//class QueueInputSpec extends StreamFlowSpecification {
//  
//  val schema = string("name") ~ int("age")
//
//  "QueueInput" should {
//    "generate data stream" in {
//      val queue = QueueInput(schema).
//        addRows(("john", 10), ("jane", 15), ("jake", 18)).
//        addRows(("john", 20), ("jane", 25), ("jake", 28)).
//        addRows(("john", 30), ("jane", 35), ("jake", 38))
//
//      runAndAssertOutput(queue, 0, 3,
//        Set(("john", 10), ("jane", 15), ("jake", 18)),
//        Set(("john", 20), ("jane", 25), ("jake", 28)),
//        Set(("john", 30), ("jane", 35), ("jake", 38)))
//        
//      assertSchema(schema, queue, 0)
//    }
//  }
//}