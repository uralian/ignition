package com.ignition.util

import scala.xml._

/**
 * XML utility functions.
 *
 * @author Vlad Orzhekhovskiy
 */
object XmlUtils {

  /**
   * Extends the standard xml NodeSeq functionality by providing typed and optional accessors.
   */
  implicit class RichNodeSeq(val xml: NodeSeq) extends AnyVal {

    def asString: String = xml.head
    def getAsString: Option[String] = nodeSeqToOpt(xml)

    def asInt: Int = xml.head
    def getAsInt: Option[Int] = nodeSeqToOpt(xml)
    
    def asDouble: Double = xml.head
    def getAsDouble: Option[Double] = nodeSeqToOpt(xml)

    def asBoolean: Boolean = xml.head
    def getAsBoolean: Option[Boolean] = nodeSeqToOpt(xml)

    implicit private def nodeToString(xml: Node): String = xml.text
    implicit private def nodeToInt(xml: Node): Int = xml.text.toInt
    implicit private def nodeToDouble(xml: Node): Double = xml.text.toDouble
    implicit private def nodeToBoolean(xml: Node): Boolean = xml.text.toBoolean

    private def nodeSeqToOpt[T](xml: NodeSeq)(implicit cnv: Node => T) = xml.headOption map cnv
  }

  /*
   * Converts various types into Text.
   */
  implicit def stringToText(s: String) = Text(s)
  implicit def booleanToText(b: Boolean) = Text(b.toString)
  implicit def intToText(n: Int) = Text(n.toString)
  implicit def optToOptText[T](o: Option[T])(implicit cnv: T => Text): Option[Text] = o map cnv
}