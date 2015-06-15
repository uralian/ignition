package com.ignition.stream

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

import com.ignition.frame.{ BasicStats, DebugOutput, Formula, Reduce }
import com.ignition.frame.{ SelectValues, StringToLiteral, SubFlow }
import com.ignition.script.RichString
import com.ignition.types.{ fieldToRichStruct, int }

@RunWith(classOf[JUnitRunner])
class TransformSpec extends StreamFlowSpecification {
  sequential

  val schema = int("a") ~ int("b")

  val queue = QueueInput(schema).
    addRows((2, 3), (4, 2)).
    addRows((2, 2), (3, 1)).
    addRows((1, 2), (2, 3))

  "Transform for FrameTransformer" should {
    "work with BasicStats" in {
      import com.ignition.frame.BasicAggregator._
      val tx = Transform(BasicStats() aggr ("a", SUM) aggr ("b", MAX))
      queue --> tx
      runAndAssertOutput(tx, 0, 3, Set((6, 3)), Set((5, 2)), Set((3, 3)))
    }
    "work with DebugOutput" in {
      val tx = Transform(DebugOutput(true, true))
      queue --> tx
      runAndAssertOutput(tx, 0, 3, Set((2, 3), (4, 2)), Set((2, 2), (3, 1)),
        Set((1, 2), (2, 3)))
    }
    "work with Formula" in {
      val tx = Transform(Formula("ab" -> "a * b".mvel))
      queue --> tx
      runAndAssertOutput(tx, 0, 3, Set((2, 3, 6), (4, 2, 8)),
        Set((2, 2, 4), (3, 1, 3)), Set((1, 2, 2), (2, 3, 6)))
    }
    "work with Reduce" in {
      import com.ignition.frame.ReduceOp._
      val tx = Transform(Reduce("a" -> SUM, "b" -> MAX))
      queue --> tx
      runAndAssertOutput(tx, 0, 3, Set((6, 3)), Set((5, 2)), Set((3, 3)))
    }
    "work with SelectValues" in {
      val tx = Transform(SelectValues() rename ("a" -> "A") retype ("b" -> "double"))
      queue --> tx
      runAndAssertOutput(tx, 0, 3, Set((2, 3.0), (4, 2.0)), Set((2, 2.0), (3, 1.0)),
        Set((1, 2.0), (2, 3.0)))
    }
  }

  "Transform for FrameSplitter" should {
    "work with Filter" in {
      val tx = Transform(com.ignition.frame.Filter($"a" == 2))
      queue --> tx
      runAndAssertOutput(tx, 0, 3, Set((2, 3)), Set((2, 2)), Set((2, 3)))
      runAndAssertOutput(tx, 1, 3, Set((4, 2)), Set((3, 1)), Set((1, 2)))
    }
  }

  "Transform for SubFlow" should {
    "work with arbitrary sub-flows" in {

      val flow = SubFlow(1, 2) { (input, output) =>
        import com.ignition.frame.ReduceOp._
        val formula = Formula("ab" -> "a * b".mvel)
        val select = SelectValues() rename ("ab" -> "total") retain ("a", "total")
        val reduce = Reduce("a" -> SUM, "total" -> MAX)
        val filter = com.ignition.frame.Filter($"a_SUM" > 4)
        input --> formula --> select --> reduce --> filter --> (output.in(0), output.in(1))
      }

      val tx = Transform(flow)
      queue --> tx

      runAndAssertOutput(tx, 0, 3, Set((6, 8)), Set((5, 4)), Set())
      runAndAssertOutput(tx, 1, 3, Set(), Set(), Set((3, 6)))
    }
  }
}