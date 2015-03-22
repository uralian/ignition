package com.ignition.types

import java.sql.{ Date, Timestamp }
import java.text.SimpleDateFormat

import scala.xml.{ NodeSeq, Text }

import org.apache.spark.sql.types.{ BinaryType, BooleanType, ByteType, DataType, DateType, Decimal, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, TimestampType }

import com.ignition.util.XmlUtils.RichNodeSeq

/**
 * Type utility functions.
 *
 * @author Vlad Orzhekhovskiy
 */
object TypeUtils {
  private val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
  private val timeFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ")

  /**
   * Resolves a data type from an arbitrary value.
   */
  def typeForValue(obj: Any): DataType = obj match {
    case _: Binary => BinaryType
    case _: Boolean => BooleanType
    case _: String => StringType
    case _: Byte => ByteType
    case _: Short => ShortType
    case _: Int => IntegerType
    case _: Long => LongType
    case _: Float => FloatType
    case _: Double => DoubleType
    case _: Decimal => DecType
    case _: Date => DateType
    case _: Timestamp => TimestampType
    case _ => throw new IllegalArgumentException(s"Invalid data type: $obj")
  }

  /**
   * Resolves a data type from a type name.
   */
  def typeForName(name: String): DataType = Symbol(name) match {
    case 'binary => BinaryType
    case 'boolean => BooleanType
    case 'string => StringType
    case 'byte => ByteType
    case 'short => ShortType
    case 'integer => IntegerType
    case 'long => LongType
    case 'float => FloatType
    case 'double => DoubleType
    case 'decimal => DecType
    case 'date => DateType
    case 'timestamp => TimestampType
    case _ => throw new IllegalArgumentException(s"Invalid data type name: $name")
  }

  /**
   * Converts a value to XML.
   */
  def valueToXml(obj: Any): NodeSeq = obj match {
    case null => NodeSeq.Empty
    case x: Binary => Text(x.mkString(","))
    case x: Boolean => Text(x.toString)
    case x: String => Text(x)
    case x: Byte => Text(x.toString)
    case x: Short => Text(x.toString)
    case x: Int => Text(x.toString)
    case x: Long => Text(x.toString)
    case x: Float => Text(x.toString)
    case x: Double => Text(x.toString)
    case x: Decimal => Text(x.toString)
    case x: Date => Text(dateFormat.format(x))
    case x: Timestamp => Text(timeFormat.format(x))
    case _ => throw new IllegalArgumentException(s"Invalid data type: $obj")
  }

  /**
   * Parses the XML using the specified data type.
   */
  def xmlToValue(dataType: DataType, xml: NodeSeq): Any = dataType match {
    case BinaryType => xml.getAsString map parseBinary orNull
    case BooleanType => xml.getAsBoolean orNull
    case StringType => xml.getAsString orNull
    case ByteType => xml.getAsInt map (_.toByte) orNull
    case ShortType => xml.getAsInt map (_.toShort) orNull
    case IntegerType => xml.getAsInt orNull
    case LongType => xml.getAsInt map (_.toLong) orNull
    case FloatType => xml.getAsDouble map (_.toFloat) orNull
    case DoubleType => xml.getAsDouble orNull
    case DecType => xml.getAsString map Decimal.apply orNull
    case DateType => xml.getAsString map parseDate orNull
    case TimestampType => xml.getAsString map parseTimestamp orNull
    case _ => throw new IllegalArgumentException(s"Invalid data type: $dataType")
  }

  private def parseBinary(str: String) = str.split(",").map(_.toByte)
  private def parseDate(str: String) = new Date(dateFormat.parse(str).getTime)
  private def parseTimestamp(str: String) = new Timestamp(timeFormat.parse(str).getTime)
}