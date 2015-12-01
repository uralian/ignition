package com.ignition

import scala.xml.{ Attribute, Elem, Null, Text }

import org.json4s.{ JObject, JValue }
import org.json4s.JsonDSL._
import org.json4s.jvalue2monadic

import com.ignition.util.JsonUtils
import com.ignition.util.XmlUtils.intToText

/**
 * A trivial implementation of a ConnectionSource which returns a static value.
 */
private[ignition] case class ConnectionSourceStub[T, R](value: T) extends ConnectionSource[T, R] {
  val step = null
  val index = 0
  def value(preview: Boolean)(implicit runtime: R): T = value
}

/**
 * Represents a connection between two steps.
 */
private[ignition] case class Connection[T, R](srcStep: Step[T, R], srcPort: Int, tgtStep: Step[T, R], tgtPort: Int)

/**
 * Base subflow trait. Contains helper methods for enumerating steps and connections.
 */
trait SubFlow[T, R] {

  /**
   * Input connection points from inner steps.
   */
  def inPoints: Iterable[ConnectionTarget[T, R]]

  /**
   * Output connection points from inner steps.
   */
  def outPoints: Iterable[ConnectionSource[T, R]]

  /**
   * Collects all steps starting with the targets and going back through the predecessors.
   */
  lazy val steps = withPredecessors(outPoints map (_.step) toSet)

  /**
   * Collects all connections given a set of steps.
   */
  lazy val connections = for {
    tgtStep <- steps
    inPorts = ins(tgtStep).zipWithIndex
    inPort <- inPorts if inPort._1.inbound != null
    ib = inPort._1.inbound if ib != null
    step = ib.step if step != null
  } yield Connection(step, ib.index, tgtStep, inPort._2)

  /**
   * The default implementation uses default id generator.
   */
  def toXml: Elem = outputToXml()(new SubFlow.DefaultIdGen)

  /**
   * The default implementation uses default id generator.
   */
  def toJson: JValue = outputToJson()(new SubFlow.DefaultIdGen)

  /**
   * Outputs the subflow into XML.
   */
  protected def outputToXml(tag: String = SubFlow.tag)(implicit idGen: (Step[T, R] => String)): Elem = {
    val stepIdMap = steps map (s => s -> idGen(s)) toMap

    <node>
      <steps>
        {
          stepIdMap map { case (step, id) => step.toXml % Attribute(None, "id", Text(id), Null) }
        }
      </steps>
      <connections>
        {
          connections map {
            case Connection(src, srcPort, tgt, tgtPort) =>
              <connect src={ stepIdMap(src) } srcPort={ srcPort } tgt={ stepIdMap(tgt) } tgtPort={ tgtPort }/>
          }
        }
      </connections>
      <in-points>
        {
          inPoints map (p => <step id={ stepIdMap(p.step) } port={ p.index }/>)
        }
      </in-points>
      <out-points>
        {
          outPoints map (p => <step id={ stepIdMap(p.step) } port={ p.index }/>)
        }
      </out-points>
    </node>.copy(label = tag)
  }

  /**
   * Outputs subflow into JSON.
   */
  def outputToJson(tag: String = SubFlow.tag)(implicit idGen: (Step[T, R] => String)): JValue = {
    val stepIdMap = steps map (s => s -> idGen(s)) toMap

    ("tag" -> tag) ~
      ("steps" -> stepIdMap.toList.map { case (step, id) => (("id" -> id): JObject) merge step.toJson }) ~
      ("connections" -> connections.map {
        case Connection(src, srcPort, tgt, tgtPort) =>
          ("src" -> stepIdMap(src)) ~ ("srcPort" -> srcPort) ~ ("tgt" -> stepIdMap(tgt)) ~ ("tgtPort" -> tgtPort)
      }) ~
      ("in-points" -> inPoints.map(tgt => ("id" -> stepIdMap(tgt.step)) ~ ("port" -> tgt.index))) ~
      ("out-points" -> outPoints.map(tgt => ("id" -> stepIdMap(tgt.step)) ~ ("port" -> tgt.index)))
  }

  /**
   * Returns the targets steps along with their predecessors, i.e. direct and indirect inbound steps.
   */
  private def withPredecessors(targets: Set[Step[T, R]]): Set[Step[T, R]] = {
    val inPorts = targets flatMap ins
    val prevSteps = inPorts map (_.inbound) filter (_ != null) map (_.step) filter (_ != null)
    if (prevSteps.isEmpty)
      targets
    else
      targets ++ withPredecessors(prevSteps)
  }
}

/**
 * SubFlow helper functions and objects.
 */
object SubFlow {
  val tag = "subflow"

  /**
   * Generates the default ID for a step by using its tag as the prefix.
   */
  class DefaultIdGen extends Function1[Step[_, _], String] {
    import com.ignition.util.JsonUtils._

    private val tagIndexMap = collection.mutable.HashMap.empty[String, Integer].withDefaultValue(0)

    def apply(step: Step[_, _]): String = {
      val tag = step.toJson \ "tag" asString
      val index = tagIndexMap(tag)
      tagIndexMap(tag) = (index + 1)
      tag + index
    }
  }
}

/**
 * A subflow which represents a Producer-type step.
 */
abstract class SubProducer[T, R](body: => ConnectionSource[T, R]) extends Producer[T, R] with SubFlow[T, R] {
  lazy val inPoints = Nil
  lazy val outPoints = List(body)

  protected def compute(preview: Boolean)(implicit runtime: R): T = body.value(preview)
}

/**
 * A subflow which represents a Transformer-type step.
 */
abstract class SubTransformer[T, R](body: => (ConnectionTarget[T, R], ConnectionSource[T, R]))
  extends Transformer[T, R] with SubFlow[T, R] {

  lazy val inPoints = List(body._1)
  lazy val outPoints = List(body._2)

  protected def compute(arg: T, preview: Boolean)(implicit runtime: R): T = {
    body._1 from ConnectionSourceStub(arg)
    body._2.value(preview)
  }
}

/**
 * A subflow which represents a Splitter-type step.
 */
abstract class SubSplitter[T, R](body: => (ConnectionTarget[T, R], Seq[ConnectionSource[T, R]]))
  extends Splitter[T, R] with SubFlow[T, R] {

  lazy val inPoints = List(body._1)
  lazy val outPoints = body._2

  def outputCount: Int = body._2.size

  protected def compute(arg: T, index: Int, preview: Boolean)(implicit runtime: R): T = {
    body._1 from ConnectionSourceStub(arg)
    body._2(index).value(preview)
  }
}

/**
 * A subflow which represents a Merger-type step.
 */
abstract class SubMerger[T, R](body: => (Seq[ConnectionTarget[T, R]], ConnectionSource[T, R]))
  extends Merger[T, R] with SubFlow[T, R] {

  override val allInputsRequired = false

  lazy val inPoints = body._1
  lazy val outPoints = List(body._2)

  def inputCount: Int = body._1.size

  protected def compute(args: IndexedSeq[T], preview: Boolean)(implicit runtime: R): T = {
    args zip body._1 foreach { case (arg, port) => port from ConnectionSourceStub(arg) }
    body._2.value(preview)
  }
}

/**
 * A subflow which represents a generic multi-input, multi-output step.
 */
abstract class SubModule[T, R](body: => (Seq[ConnectionTarget[T, R]], Seq[ConnectionSource[T, R]]))
  extends Module[T, R] with SubFlow[T, R] {

  override val allInputsRequired = false

  lazy val inPoints = body._1
  lazy val outPoints = body._2

  def inputCount: Int = body._1.size
  def outputCount: Int = body._2.size

  protected def compute(args: IndexedSeq[T], index: Int, preview: Boolean)(implicit runtime: R): T = {
    args zip body._1 foreach { case (arg, port) => port from ConnectionSourceStub(arg) }
    body._2(index).value(preview)
  }
}