package com.ignition.frame

import scala.xml.{ Attribute, Elem, Node, Null, Text }
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.json4s.JValue
import org.json4s.JsonDSL._
import org.json4s.jvalue2monadic
import com.ignition.{ SparkRuntime, Step, AbstractStep }
import com.ignition.util.JsonUtils.RichJValue
import com.ignition.util.XmlUtils.{ RichNodeSeq, intToText }
import SubFlow.{ FlowInput, FlowOutput, FlowStep, tag }

/**
 * Sub-flow - a collection of connected steps with designated input and output.
 */
case class SubFlow(override val inputCount: Int, override val outputCount: Int)(body: (FlowInput, FlowOutput) => Unit)
  extends FrameModule(inputCount, outputCount) {

  private val input = FlowInput(inputCount)
  private val output = FlowOutput(outputCount)
  body(input, output)

  private val tagIndexMap = collection.mutable.HashMap.empty[String, Integer].withDefaultValue(0)

  private val steps = (collectSteps(output) - input) map { step => step -> issueId(step) } toMap
  private val connections = for {
    (tgtStep, tgtId) <- steps + (output -> "OUTPUT")
    ((srcStep, srcPort), tgtPort) <- tgtStep.ins.zipWithIndex
    srcId = if (srcStep eq input) "INPUT" else steps(srcStep.asInstanceOf[FlowStep])
  } yield (srcId, srcPort, tgtId, tgtPort)

  override def connectFrom(inIndex: Int, step: Step[DataFrame], outIndex: Int): this.type = {
    input.connectFrom(inIndex, step, outIndex)
    this
  }

  override def output(index: Int, limit: Option[Int] = None)(implicit runtime: SparkRuntime): DataFrame =
    output.output(index, limit)

  override def outSchema(index: Int)(implicit runtime: SparkRuntime): StructType =
    wrap { output.outSchema(index) }

  /* not used */
  protected def compute(args: Seq[DataFrame], index: Int, limit: Option[Int])(implicit runtime: SparkRuntime): DataFrame = ???

  /* not used */
  protected def computeSchema(inSchemas: Seq[StructType], index: Int)(implicit runtime: SparkRuntime): StructType = ???

  def toXml: Elem =
    <node>
      <steps>
        {
          steps map { case (step, id) => step.toXml % Attribute(None, "id", Text(id), Null) }
        }
      </steps>
      <connections>
        {
          connections map {
            case (src, srcPort, tgt, tgtPort) =>
              <connect src={ src } srcPort={ srcPort } tgt={ tgt } tgtPort={ tgtPort }/>
          }
        }
      </connections>
    </node>.copy(label = tag)

  def toJson: JValue = ("tag" -> tag) ~
    ("steps" -> steps.map { case (step, id) => ("id" -> id) ++ step.toJson }) ~
    ("connections" -> connections.map {
      case (src, srcPort, tgt, tgtPort) =>
        ("src" -> src) ~ ("srcPort" -> srcPort) ~ ("tgt" -> tgt) ~ ("tgtPort" -> tgtPort)
    })

  private def collectSteps(target: FlowStep): Set[FlowStep] = {
    val sources = target.ins collect {
      case (step, _) if step != null => step.asInstanceOf[FlowStep]
    } diff List(input)

    val all = sources flatMap collectSteps
    (sources ++ all).toSet
  }

  private def issueId(step: FlowStep): String = {
    val tag = step.toJson \ "tag" asString
    val index = tagIndexMap(tag)
    tagIndexMap(tag) = (index + 1)
    tag + index
  }
}

/**
 * SubFlow companion object.
 */
object SubFlow {
  val tag = "subflow"

  type FlowStep = FrameStep with AbstractStep[DataFrame]

  def fromXml(xml: Node) = {
    val stepById = (xml \ "steps" \ "_") map { node =>
      val id = node \ "@id" asString
      val step = StepFactory.fromXml(node).asInstanceOf[FlowStep]
      id -> step
    } toMap

    val maxInputPort = (xml \ "connections" \ "connect") filter { n =>
      (n \ "@src" asString) == "INPUT"
    } map { n => n \ "@srcPort" asInt } max

    val maxOutputPort = (xml \ "connections" \ "connect") filter { n =>
      (n \ "@tgt" asString) == "OUTPUT"
    } map { n => n \ "@tgtPort" asInt } max

    SubFlow(maxInputPort + 1, maxOutputPort + 1)(
      (input, output) => xml \ "connections" \ "connect" foreach { n =>
        val src = n \ "@src" asString
        val srcStep = if (src == "INPUT") input else stepById(src)
        val srcPort = n \ "@srcPort" asInt
        val tgt = n \ "@tgt" asString
        val tgtStep = if (tgt == "OUTPUT") output else stepById(tgt)
        val tgtPort = n \ "@tgtPort" asInt

        tgtStep.connectFrom(tgtPort, srcStep, srcPort)
      })
  }

  def fromJson(json: JValue) = {
    val stepById = (json \ "steps" asArray) map { node =>
      val id = node \ "id" asString
      val step = StepFactory.fromJson(node).asInstanceOf[FlowStep]
      id -> step
    } toMap

    val maxInputPort = (json \ "connections" asArray) filter { n =>
      (n \ "src" asString) == "INPUT"
    } map { n => n \ "srcPort" asInt } max

    val maxOutputPort = (json \ "connections" asArray) filter { n =>
      (n \ "tgt" asString) == "OUTPUT"
    } map { n => n \ "tgtPort" asInt } max

    SubFlow(maxInputPort + 1, maxOutputPort + 1)(
      (input, output) => (json \ "connections" asArray) foreach { n =>
        val src = n \ "src" asString
        val srcStep = if (src == "INPUT") input else stepById(src)
        val srcPort = n \ "srcPort" asInt
        val tgt = n \ "tgt" asString
        val tgtStep = if (tgt == "OUTPUT") output else stepById(tgt)
        val tgtPort = n \ "tgtPort" asInt

        tgtStep.connectFrom(tgtPort, srcStep, srcPort)
      })
  }

  /**
   * Flow input data.
   */
  private case class FlowInput(override val inputCount: Int) extends FrameModule(inputCount, inputCount) {

    override val allInputsRequired: Boolean = false

    protected def compute(args: Seq[DataFrame], index: Int,
                          limit: Option[Int])(implicit runtime: SparkRuntime): DataFrame = args(index)

    protected def computeSchema(inSchemas: Seq[StructType], index: Int)(implicit runtime: SparkRuntime): StructType =
      computedSchema(index)

    def toXml: Elem = ???
    def toJson: JValue = ???
  }

  /**
   * Flow output data.
   */
  private case class FlowOutput(override val outputCount: Int) extends FrameModule(outputCount, outputCount) {

    protected def compute(args: Seq[DataFrame], index: Int,
                          limit: Option[Int])(implicit runtime: SparkRuntime): DataFrame = args(index)

    protected def computeSchema(inSchemas: Seq[StructType], index: Int)(implicit runtime: SparkRuntime): StructType =
      inSchemas(index)

    def toXml: Elem = ???
    def toJson: JValue = ???
  }

  def apply(outputCount: Int)(body: (FlowOutput) => Unit): SubFlow = {
    val func = (in: FlowInput, out: FlowOutput) => body(out)
    apply(0, outputCount)(func)
  }
}