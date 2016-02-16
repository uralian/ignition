package com.ignition.samples

import com.ignition.frame.DebugOutput
import com.ignition.stream
import com.ignition.stream.{ MvelMapListStateUpdate, MvelMapStateUpdate, QueueInput, StreamFlow, foreach }
import com.ignition.types.{ RichStructType, fieldToRichStruct, int, string }

/**
 * @author Vlad Orzhekhovskiy
 */
object StreamStateUpdate extends App {

  val flow = StreamFlow {

    val schema = string("name") ~ string("topic") ~ int("score")
    val queue = QueueInput(schema).
      addRows(("john", "a", 65), ("jake", "c", 78)).
      addRows(("jane", "b", 55), ("jane", "a", 76), ("jake", "b", 84)).
      addRows().
      addRows(("jill", "b", 77), ("john", "c", 95)).
      addRows(("jake", "c", 86), ("jake", "b", 59), ("jane", "d", 88)).
      addRows(("john", "d", 97)).
      addRows(("josh", "a", 84), ("jane", "c", 65))

    // this state function remembers the last topic and score per name
    val stateSchema1 = string("topic") ~ int("score")
    val expr1 = """$input.empty ? $state : $input[$input.size() - 1]"""
    val updateState1 = MvelMapStateUpdate(stateSchema1) code expr1 keys "name"
    val debug1 = foreach(DebugOutput() title "last topic and score for name")

    // this state function accumulates all scores for a given topic
    val stateSchema2 = int("score").schema
    val expr2 = """
      | newState = $state;
      | 
      | if ($input.size() > 0) {
      |   newRows = new java.util.ArrayList();
      |   foreach (row: $input)
      |     newRows.add(["score" : row.score]);
      |   
      |   if ($state == empty)
      |     newState = newRows;
      |   else
      |     newState.addAll(newRows);
      | }
      | 
      | newState;
      """.stripMargin
    val updateState2 = MvelMapListStateUpdate(stateSchema2) code expr2 keys "topic"
    val debug2 = foreach(DebugOutput() title "all scores for topic")

    queue --> updateState1 --> debug1
    queue --> updateState2 --> debug2

    (debug1, debug2)
  }

  stream.Main.startAndWait(flow)
}