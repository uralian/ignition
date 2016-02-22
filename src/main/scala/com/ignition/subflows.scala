package com.ignition

import scala.xml.{ Attribute, Elem, Node, Null, Text }

import org.json4s.{ JObject, JValue }
import org.json4s.JsonDSL._
import org.json4s.jvalue2monadic

import com.ignition.util.JsonUtils._
import com.ignition.util.XmlUtils._

/**
 * A trivial implementation of a ConnectionSource which returns a static value.
 */
private[ignition] case class ConnectionSourceStub[T, R <: FlowRuntime](v: T) extends ConnectionSource[T, R] {
  val step = null
  val index = 0
  def value(implicit runtime: R): T = v
}

/**
 * Represents a connection between two steps.
 */
private[ignition] case class Connection[T, R <: FlowRuntime](srcStep: Step[T, R], srcPort: Int, tgtStep: Step[T, R], tgtPort: Int)

/**
 * Base subflow trait. Contains helper methods for enumerating steps and connections.
 */
trait SubFlow[T, R <: FlowRuntime] extends Step[T, R] {

  /**
   * Tag used for serialization.
   */
  def tag: String

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
  def toXml: Elem = outputToXml(new SubFlow.DefaultIdGen)

  /**
   * The default implementation uses default id generator.
   */
  def toJson: JValue = outputToJson(new SubFlow.DefaultIdGen)

  /**
   * Outputs the subflow into XML.
   */
  protected def outputToXml(implicit idGen: (Step[T, R] => String)): Elem = {
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
  def outputToJson(implicit idGen: (Step[T, R] => String)): JValue = {
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

  /**
   * Reset the cache of the subflow and its constituents.
   */
  override private[ignition] def resetCache(predecessors: Boolean, descendants: Boolean): Unit = synchronized {
    super.resetCache(predecessors, descendants)
    steps foreach (_.resetCache(false, false))
  }
}

/**
 * SubFlow helper functions and objects.
 */
object SubFlow {

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
 * Creates subflows from Xml and Json.
 * @param S step class.
 * @param T type encapsulating the data that is passed between steps.
 * @param R type of the runtime context passed to the node for evaluation.
 */
trait SubFlowFactory[S <: Step[T, R], T, R <: FlowRuntime] {

  /**
   * Factory that can build steps from XML.
   */
  def xmlFactory: XmlStepFactory[S, T, R]

  /**
   * Factory that can build steps from JSON.
   */
  def jsonFactory: JsonStepFactory[S, T, R]

  /**
   * Creates a new subflow instance from the supplied input and output connection points.
   */
  def instantiate(inPoints: Seq[ConnectionTarget[T, R]], outPoints: Seq[ConnectionSource[T, R]]): S with SubFlow[T, R]

  /**
   * Creates a subflow from XML.
   */
  def fromXml(xml: Node): S with SubFlow[T, R] = {
    val stepById = (xml \ "steps" \ "_") map { node =>
      val id = node \ "@id" asString
      val step = xmlFactory.fromXml(node)
      id -> step
    } toMap

    xml \ "connections" \ "connect" foreach { n =>
      val srcStep = stepById(n \ "@src" asString)
      val srcPort = n \ "@srcPort" asInt
      val tgtStep = stepById(n \ "@tgt" asString)
      val tgtPort = n \ "@tgtPort" asInt

      outs(srcStep)(srcPort) --> ins(tgtStep)(tgtPort)
    }

    val inPoints = xml \ "in-points" \ "step" map { n =>
      val srcStep = stepById(n \ "@id" asString)
      val srcPort = n \ "@port" asInt

      ins(srcStep)(srcPort)
    }

    val outPoints = xml \ "out-points" \ "step" map { n =>
      val tgtStep = stepById(n \ "@id" asString)
      val tgtPort = n \ "@port" asInt

      outs(tgtStep)(tgtPort)
    }

    instantiate(inPoints, outPoints)
  }

  /**
   * Creates a subflow from JSON.
   */
  def fromJson(json: JValue) = {
    val stepById = (json \ "steps" asArray) map { node =>
      val id = node \ "id" asString
      val step = jsonFactory.fromJson(node)
      id -> step
    } toMap

    (json \ "connections" asArray) foreach { n =>
      val srcStep = stepById(n \ "src" asString)
      val srcPort = n \ "srcPort" asInt
      val tgtStep = stepById(n \ "tgt" asString)
      val tgtPort = n \ "tgtPort" asInt

      outs(srcStep)(srcPort) --> ins(tgtStep)(tgtPort)
    }

    val inPoints = (json \ "in-points" asArray) map { n =>
      val srcStep = stepById(n \ "id" asString)
      val srcPort = n \ "port" asInt

      ins(srcStep)(srcPort)
    }

    val outPoints = (json \ "out-points" asArray) map { n =>
      val tgtStep = stepById(n \ "id" asString)
      val tgtPort = n \ "port" asInt

      outs(tgtStep)(tgtPort)
    }

    instantiate(inPoints, outPoints)
  }
}

/**
 * A subflow which represents a Producer-type step.
 */
abstract class SubProducer[T, R <: FlowRuntime](body: => ConnectionSource[T, R]) extends Producer[T, R] with SubFlow[T, R] {
  lazy val inPoints = Nil
  lazy val outPoints = List(body)

  protected def compute(implicit runtime: R): T = body.value
}

/**
 * A subflow which represents a Transformer-type step.
 */
abstract class SubTransformer[T, R <: FlowRuntime](body: => (ConnectionTarget[T, R], ConnectionSource[T, R]))
    extends Transformer[T, R] with SubFlow[T, R] {

  lazy val inPoints = List(body._1)
  lazy val outPoints = List(body._2)

  protected def compute(arg: T)(implicit runtime: R): T = {
    body._1 from ConnectionSourceStub(arg)
    body._2.value
  }
}

/**
 * A subflow which represents a Splitter-type step.
 */
abstract class SubSplitter[T, R <: FlowRuntime](body: => (ConnectionTarget[T, R], Seq[ConnectionSource[T, R]]))
    extends Splitter[T, R] with SubFlow[T, R] {

  lazy val inPoints = List(body._1)
  lazy val outPoints = body._2

  def outputCount: Int = body._2.size

  protected def compute(arg: T, index: Int)(implicit runtime: R): T = {
    body._1 from ConnectionSourceStub(arg)
    body._2(index).value
  }
}

/**
 * A subflow which represents a Merger-type step.
 */
abstract class SubMerger[T, R <: FlowRuntime](body: => (Seq[ConnectionTarget[T, R]], ConnectionSource[T, R]))
    extends Merger[T, R] with SubFlow[T, R] {

  override val allInputsRequired = false

  lazy val inPoints = body._1
  lazy val outPoints = List(body._2)

  def inputCount: Int = body._1.size

  protected def compute(args: IndexedSeq[T])(implicit runtime: R): T = {
    args zip body._1 foreach { case (arg, port) => port from ConnectionSourceStub(arg) }
    body._2.value
  }
}

/**
 * A subflow which represents a generic multi-input, multi-output step.
 */
abstract class SubModule[T, R <: FlowRuntime](body: => (Seq[ConnectionTarget[T, R]], Seq[ConnectionSource[T, R]]))
    extends Module[T, R] with SubFlow[T, R] {

  override val allInputsRequired = false

  lazy val inPoints = body._1
  lazy val outPoints = body._2

  def inputCount: Int = body._1.size
  def outputCount: Int = body._2.size

  protected def compute(args: IndexedSeq[T], index: Int)(implicit runtime: R): T = {
    args zip body._1 foreach { case (arg, port) => port from ConnectionSourceStub(arg) }
    body._2(index).value
  }
}