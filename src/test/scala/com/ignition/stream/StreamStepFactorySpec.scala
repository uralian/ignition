package com.ignition.stream

import scala.xml.{ Elem, Node }

import org.json4s.JValue
import org.json4s.JsonDSL._
import org.json4s.jvalue2monadic
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

import com.ignition.util.JsonUtils.RichJValue
import com.ignition.util.XmlUtils.RichNodeSeq

@RunWith(classOf[JUnitRunner])
class StreamStepFactorySpec extends StreamFlowSpecification {
  sequential

  "StreamStepFactory" should {
    "pre-register xml factories from configuration" in {
      StreamStepFactory.fromXml(<test1><a>abc</a><b>5</b></test1>) === TestStep1("abc", 5)
      StreamStepFactory.fromXml(<test2><x>true</x></test2>) === TestStep2(true)
    }
    "pre-register json factories from configuration" in {
      StreamStepFactory.fromJson(("tag" -> "test1") ~ ("a" -> "abc") ~ ("b" -> 5)) === TestStep1("abc", 5)
      StreamStepFactory.fromJson(("tag" -> "test2") ~ ("x" -> true)) === TestStep2(true)
    }
  }

  "StreamStepFactory.registerXml" should {
    "register object-based xml factories" in {
      StreamStepFactory.registerXml("test1a", TestStep1.fromXml)
      StreamStepFactory.fromXml(<test1a><a>abc</a><b>5</b></test1a>) === TestStep1("abc", 5)
    }
    "register class-based xml factories" in {
      StreamStepFactory.registerXml("test2a", new TestStep2XmlFactory().fromXml)
      StreamStepFactory.fromXml(<test2a><x>true</x></test2a>) === TestStep2(true)
    }
  }

  "StreamStepFactory.registerJson" should {
    "register object-based json factories" in {
      StreamStepFactory.registerJson("test1b", TestStep1.fromJson)
      StreamStepFactory.fromJson(("tag" -> "test1b") ~ ("a" -> "abc") ~ ("b" -> 5)) === TestStep1("abc", 5)
    }
    "register class-based json factories" in {
      StreamStepFactory.registerJson("test2b", new TestStep2JsonFactory().fromJson)
      StreamStepFactory.fromJson(("tag" -> "test2b") ~ ("x" -> true)) === TestStep2(true)
    }
  }
}

/**
 * Abstract test step class.
 */
abstract class AbstractTestStep extends StreamTransformer {
  protected def compute(arg: DataStream)(implicit runtime: SparkStreamingRuntime): DataStream = ???
}

/**
 * Test step 1.
 */
case class TestStep1(a: String, b: Int) extends AbstractTestStep {
  def toXml: Elem = <node><a>{ a }</a><b>{ b }</b></node>.copy(label = TestStep1.tag)
  def toJson: JValue = ("tag" -> TestStep1.tag) ~ ("a" -> a) ~ ("b" -> b)
}

/**
 * Test step 2.
 */
case class TestStep2(x: Boolean) extends AbstractTestStep {
  def toXml: Elem = <node><x>{ x }</x></node>.copy(label = TestStep2.tag)
  def toJson: JValue = ("tag" -> TestStep2.tag) ~ ("x" -> x)
}

/**
 * Provides both XML and JSON factories for TestStep1.
 */
object TestStep1 extends XmlStreamStepFactory with JsonStreamStepFactory {
  val tag = "test1"

  def fromXml(xml: Node) = {
    val a = xml \ "a" asString
    val b = xml \ "b" asInt

    apply(a, b)
  }

  def fromJson(json: JValue) = {
    val a = json \ "a" asString
    val b = json \ "b" asInt

    apply(a, b)
  }
}

/**
 * Provides tag for TestStep2.
 */
object TestStep2 {
  val tag = "test2"
}

/**
 * XML factory for TestStep2.
 */
class TestStep2XmlFactory extends XmlStreamStepFactory {
  def fromXml(xml: Node) = new TestStep2(xml \ "x" asBoolean)
}

/**
 * JSON factory for TestStep2.
 */
class TestStep2JsonFactory extends JsonStreamStepFactory {
  def fromJson(json: JValue) = new TestStep2(json \ "x" asBoolean)
}