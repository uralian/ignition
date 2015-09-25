package com.ignition.frame

import scala.xml.{ Attribute, Elem, Node, Null, Text }
import org.json4s.JValue
import org.json4s.JsonDSL._
import org.json4s.jvalue2monadic
import com.ignition._
import com.ignition.util.JsonUtils.RichJValue
import com.ignition.util.XmlUtils.{ RichNodeSeq, intToText }
import org.apache.spark.sql.DataFrame

/**
 * Data Flow represents an executable job.
 *
 * @author Vlad Orzhekhovskiy
 */
case class DataFlow(targets: Iterable[FrameStep]) {
  import DataFlow._

  private val steps = targets.flatMap(tgt => collectSteps(tgt.asInstanceOf[FlowStep])) ++
    targets.map(_.asInstanceOf[FlowStep])

  private val connections = for {
    tgtStep <- steps
    ((srcStep, srcPort), tgtPort) <- tgtStep.ins.zipWithIndex
  } yield (srcStep.asInstanceOf[FlowStep], srcPort, tgtStep, tgtPort)

  /**
   * Executes a data flow.
   */
  def execute(implicit runtime: SparkRuntime): Unit = (for {
    tgt <- targets
    index <- 0 until tgt.outputCount
  } yield (tgt, index)) foreach {
    case (tgt, index) => tgt.output(index)
  }

  def toXml(implicit idGen: FlowStep => String = defaultIdGen): Elem = {
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
            case (src, srcPort, tgt, tgtPort) =>
              <connect src={ stepIdMap(src) } srcPort={ srcPort } tgt={ stepIdMap(tgt) } tgtPort={ tgtPort }/>
          }
        }
      </connections>
      <targets>
        {
          targets map { tgt => <step id={ stepIdMap(tgt.asInstanceOf[FlowStep]) }/> }
        }
      </targets>
    </node>.copy(label = tag)
  }

  def toJson(implicit idGen: FlowStep => String = defaultIdGen): JValue = {
    val stepIdMap = steps map (s => s -> idGen(s)) toMap

    ("tag" -> tag) ~
      ("steps" -> stepIdMap.map { case (step, id) => ("id" -> id) ++ step.toJson }) ~
      ("connections" -> connections.map {
        case (src, srcPort, tgt, tgtPort) =>
          ("src" -> stepIdMap(src)) ~ ("srcPort" -> srcPort) ~ ("tgt" -> stepIdMap(tgt)) ~ ("tgtPort" -> tgtPort)
      }) ~
      ("targets" -> targets.map(tgt => stepIdMap(tgt.asInstanceOf[FlowStep])))
  }

  /**
   * Collect all predecessors of this step (i.e. direct and indirect inbound steps.
   */
  private def collectSteps(target: FlowStep): Set[FlowStep] = {
    val sources = target.ins collect {
      case (step, _) if step != null => step.asInstanceOf[FlowStep]
    }

    val all = sources flatMap collectSteps
    (sources ++ all).toSet
  }
}

/**
 * DataFlow companion object.
 */
object DataFlow {
  val tag = "dataflow"

  type FlowStep = FrameStep with AbstractStep[DataFrame]

  def apply(steps: Product): DataFlow = steps match {
    case step: FrameStep => new DataFlow(Seq(step))
    case _ => new DataFlow(steps.productIterator.asInstanceOf[Iterator[FrameStep]].toSeq)
  }

  def fromXml(xml: Node) = {
    val stepById = (xml \ "steps" \ "_") map { node =>
      val id = node \ "@id" asString
      val step = StepFactory.fromXml(node).asInstanceOf[FlowStep]
      id -> step
    } toMap

    xml \ "connections" \ "connect" foreach { n =>
      val srcStep = stepById(n \ "@src" asString)
      val srcPort = n \ "@srcPort" asInt
      val tgtStep = stepById(n \ "@tgt" asString)
      val tgtPort = n \ "@tgtPort" asInt

      tgtStep.connectFrom(tgtPort, srcStep, srcPort)
    }

    val targets = xml \ "targets" \ "step" map { n => stepById(n \ "@id" asString) }

    DataFlow(targets)
  }

  def fromJson(json: JValue) = {
    val stepById = (json \ "steps" asArray) map { node =>
      val id = node \ "id" asString
      val step = StepFactory.fromJson(node).asInstanceOf[FlowStep]
      id -> step
    } toMap

    (json \ "connections" asArray) foreach { n =>
      val srcStep = stepById(n \ "src" asString)
      val srcPort = n \ "srcPort" asInt
      val tgtStep = stepById(n \ "tgt" asString)
      val tgtPort = n \ "tgtPort" asInt

      tgtStep.connectFrom(tgtPort, srcStep, srcPort)
    }

    val targets = (json \ "targets" asArray) map { n => stepById(n asString) }

    DataFlow(targets)
  }

  private val tagIndexMap = collection.mutable.HashMap.empty[String, Integer].withDefaultValue(0)

  /**
   * Generates the default ID for a step by using its tag as the prefix.
   */
  private[DataFlow] def defaultIdGen(step: FlowStep): String = {
    val tag = step.toJson \ "tag" asString
    val index = tagIndexMap(tag)
    tagIndexMap(tag) = (index + 1)
    tag + index
  }
}