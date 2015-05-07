package com.ignition.types

import java.sql.{ Date, Timestamp }
import java.text.SimpleDateFormat

import scala.xml.{ NodeSeq, Text }

import org.apache.spark.sql.types._

import com.ignition.util.XmlUtils.RichNodeSeq

/**
 * Type utility functions.
 *
 * @author Vlad Orzhekhovskiy
 */
object TypeUtils {
  private val dateFormat = new ThreadLocal[SimpleDateFormat]() {
    override protected def initialValue = new SimpleDateFormat("yyyy-MM-dd")
  }
  private val timeFormat = new ThreadLocal[SimpleDateFormat]() {
    override protected def initialValue = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ")
  }
  
  /**
   * Returns a sample value of the specified type.
   */
  def sampleForType(dt: DataType): Any = dt match {
    case BinaryType => Array(0.toByte)
    case BooleanType => true
    case StringType => "hello"
    case ByteType => 0.toByte
    case ShortType => 0.toShort
    case IntegerType => 0.toInt
    case LongType => 0.toLong
    case FloatType => 0.toFloat
    case DoubleType => 0.toDouble
    case _: DecimalType => Decimal(0.0).toJavaBigDecimal
    case DateType => java.sql.Date.valueOf("2011-08-05")
    case TimestampType => java.sql.Timestamp.valueOf("2013-04-02 04:00:00")
    case _ => throw new IllegalArgumentException(s"Invalid data type: $dt")
  }

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
    case _: java.math.BigDecimal => DecType
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
    case 'int | 'integer => IntegerType
    case 'long => LongType
    case 'float => FloatType
    case 'double => DoubleType
    case 'decimal => DecType
    case 'date => DateType
    case 'timestamp => TimestampType
    case _ => throw new IllegalArgumentException(s"Invalid data type name: $name")
  }

  /**
   * Parses a string according to the specified data type.
   */
  def valueOf(str: String, dt: DataType) = dt match {
    case BinaryType => parseBinary(str)
    case BooleanType => str.toBoolean
    case StringType => str
    case ByteType => str.toByte
    case ShortType => str.toShort
    case IntegerType => str.toInt
    case LongType => str.toLong
    case FloatType => str.toFloat
    case DoubleType => str.toDouble
    case _: DecimalType => Decimal(str).toJavaBigDecimal
    case DateType => parseDate(str)
    case TimestampType => parseTimestamp(str)
    case _ => throw new IllegalArgumentException(s"Invalid data type: $dt")
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
    case x: java.math.BigDecimal => Text(x.toString)
    case x: Date => Text(formatDate(x))
    case x: Timestamp => Text(formatTimestamp(x))
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
    case _: DecimalType => xml.getAsString map Decimal.apply orNull
    case DateType => xml.getAsString map parseDate orNull
    case TimestampType => xml.getAsString map parseTimestamp orNull
    case _ => throw new IllegalArgumentException(s"Invalid data type: $dataType")
  }

  private def parseBinary(str: String) = str.split(",").map(_.toByte)
  private def parseDate(str: String) = new Date(dateFormat.get.parse(str).getTime)
  private def parseTimestamp(str: String) = new Timestamp(timeFormat.get.parse(str).getTime)

  private def formatDate(date: Date) = dateFormat.get.format(date)
  private def formatTimestamp(time: Timestamp) = timeFormat.get.format(time)
}